use anyhow::{Result, bail};
use std::collections::{BTreeSet, VecDeque};
use std::io::{Read, Write};

use super::disk_client::DiskClient;
use super::row_codec::decode_rows_from_block;
use crate::row::{Row, Schema};

const SCRATCH_WRITE_BATCH_BLOCKS: usize = 256;
const SCRATCH_CURSOR_BATCH_BLOCKS: usize = 64;
const SCRATCH_STREAM_BATCH_BLOCKS: usize = 256;
const SCRATCH_WRITER_ALLOCATION_CHUNK_BLOCKS: usize = 64;

#[derive(Debug, Clone)]
pub struct ScratchRun {
    block_ids: Vec<u64>,
}

pub struct ScratchRunCursor {
    run: ScratchRun,
    next_block: usize,
    buffer: VecDeque<Row>,
}

pub struct ScratchRunWriter<R: Read, W: Write> {
    disk: *mut DiskClient<R, W>,
    scratch: *mut ScratchSpace,
    current_block: Vec<u8>,
    used: usize,
    row_count: u16,
    block_ids: Vec<u64>,
    reserved_blocks: Vec<u64>,
    pending_start_block: Option<u64>,
    pending_block_count: usize,
    pending_bytes: Vec<u8>,
}

pub struct ScratchSpace {
    block_size: usize,
    next_block: u64,
    free_blocks: BTreeSet<u64>,
}

impl ScratchRun {
    pub fn new(block_ids: Vec<u64>) -> Self {
        Self { block_ids }
    }
    pub fn empty() -> Self {
        Self {
            block_ids: Vec::new(),
        }
    }
    pub fn block_ids(&self) -> &[u64] {
        &self.block_ids
    }
}

impl ScratchRunCursor {
    pub fn new(run: ScratchRun) -> Self {
        Self {
            run,
            next_block: 0,
            buffer: VecDeque::new(),
        }
    }

    pub fn next_row<R, W>(
        &mut self,
        schema: &Schema,
        disk: &mut DiskClient<R, W>,
    ) -> Result<Option<Row>>
    where
        R: Read,
        W: Write,
    {
        loop {
            if let Some(row) = self.buffer.pop_front() {
                return Ok(Some(row));
            }
            if self.next_block >= self.run.block_ids().len() {
                return Ok(None);
            }
            let batch_blocks = contiguous_batch_len(
                self.run.block_ids(),
                self.next_block,
                SCRATCH_CURSOR_BATCH_BLOCKS,
            );
            let block_size = disk.get_block_size()? as usize;
            let blocks =
                disk.read_blocks(self.run.block_ids()[self.next_block], batch_blocks as u64)?;
            self.next_block += batch_blocks;

            let mut rows = VecDeque::new();
            for block_index in 0..batch_blocks {
                let start = block_index * block_size;
                let end = start + block_size;
                rows.extend(decode_rows_from_block(&blocks[start..end], schema)?);
            }
            self.buffer = rows;
        }
    }
}

impl<R: Read, W: Write> ScratchRunWriter<R, W> {
    pub fn new(disk: *mut DiskClient<R, W>, scratch: *mut ScratchSpace, block_size: usize) -> Self {
        Self {
            disk,
            scratch,
            current_block: vec![0u8; block_size],
            used: 0,
            row_count: 0,
            block_ids: Vec::new(),
            reserved_blocks: Vec::new(),
            pending_start_block: None,
            pending_block_count: 0,
            pending_bytes: Vec::with_capacity(block_size * SCRATCH_WRITE_BATCH_BLOCKS),
        }
    }

    pub fn push_row(&mut self, row: &Row) -> Result<()> {
        let row_size = row.serialized_size();
        self.prepare_row_write(row_size)?;
        row.encode_into(&mut self.current_block[self.used..self.used + row_size])?;
        self.used += row_size;
        self.row_count += 1;
        Ok(())
    }

    pub fn push_encoded_row(&mut self, row: &[u8]) -> Result<()> {
        let row_size = row.len();
        self.prepare_row_write(row_size)?;
        self.current_block[self.used..self.used + row_size].copy_from_slice(row);
        self.used += row_size;
        self.row_count += 1;
        Ok(())
    }

    fn prepare_row_write(&mut self, row_size: usize) -> Result<()> {
        let payload = self.current_block.len() - 2;
        if row_size > payload {
            bail!("row larger than block payload");
        }
        if self.used + row_size > payload {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<ScratchRun> {
        if self.row_count > 0 {
            self.flush_block()?;
        }
        self.flush_pending_blocks()?;
        if !self.reserved_blocks.is_empty() {
            unsafe {
                (*self.scratch).release_blocks(self.reserved_blocks.drain(..));
            }
        }
        Ok(ScratchRun::new(self.block_ids))
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }
        finalize_block(&mut self.current_block, self.row_count);
        let id = self.reserve_writer_block();
        self.queue_block_write(id)?;
        self.block_ids.push(id);
        self.current_block.fill(0);
        self.used = 0;
        self.row_count = 0;
        Ok(())
    }

    fn queue_block_write(&mut self, block_id: u64) -> Result<()> {
        match self.pending_start_block {
            Some(start)
                if block_id == start + self.pending_block_count as u64
                    && self.pending_block_count < SCRATCH_WRITE_BATCH_BLOCKS =>
            {
                self.pending_bytes.extend_from_slice(&self.current_block);
                self.pending_block_count += 1;
            }
            Some(_) => {
                self.flush_pending_blocks()?;
                self.pending_start_block = Some(block_id);
                self.pending_block_count = 1;
                self.pending_bytes.extend_from_slice(&self.current_block);
            }
            None => {
                self.pending_start_block = Some(block_id);
                self.pending_block_count = 1;
                self.pending_bytes.extend_from_slice(&self.current_block);
            }
        }

        if self.pending_block_count >= SCRATCH_WRITE_BATCH_BLOCKS {
            self.flush_pending_blocks()?;
        }

        Ok(())
    }

    fn flush_pending_blocks(&mut self) -> Result<()> {
        if self.pending_block_count == 0 {
            return Ok(());
        }

        let start_block = self
            .pending_start_block
            .expect("pending start block should exist");
        unsafe {
            (*self.disk).write_blocks(
                start_block,
                self.pending_block_count as u64,
                &self.pending_bytes,
            )?;
        }
        self.pending_start_block = None;
        self.pending_block_count = 0;
        self.pending_bytes.clear();
        Ok(())
    }

    fn reserve_writer_block(&mut self) -> u64 {
        if let Some(block_id) = self.reserved_blocks.pop() {
            return block_id;
        }

        let mut blocks = unsafe {
            (*self.scratch).reserve_contiguous_blocks(SCRATCH_WRITER_ALLOCATION_CHUNK_BLOCKS)
        };
        blocks.reverse();
        self.reserved_blocks = blocks;
        self.reserved_blocks
            .pop()
            .expect("scratch chunk allocation must return at least one block")
    }
}

impl ScratchSpace {
    pub fn new(block_size: u64, anon_start: u64) -> Self {
        Self {
            block_size: block_size as usize,
            next_block: anon_start,
            free_blocks: BTreeSet::new(),
        }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn reserve_contiguous_blocks(&mut self, max_blocks: usize) -> Vec<u64> {
        let max_blocks = max_blocks.max(1);
        if let Some(blocks) = self.take_contiguous_free_blocks(max_blocks) {
            return blocks;
        }

        let start = self.next_block;
        self.next_block += max_blocks as u64;
        (start..start + max_blocks as u64).collect()
    }

    pub fn release_run(&mut self, run: ScratchRun) {
        self.free_blocks.extend(run.block_ids);
    }

    pub fn release_blocks(&mut self, blocks: impl IntoIterator<Item = u64>) {
        self.free_blocks.extend(blocks);
    }

    fn take_contiguous_free_blocks(&mut self, max_blocks: usize) -> Option<Vec<u64>> {
        if self.free_blocks.is_empty() {
            return None;
        }

        let mut best_start = None;
        let mut best_len = 0usize;
        let mut current_start = 0u64;
        let mut current_len = 0usize;
        let mut previous = None;

        for &block in &self.free_blocks {
            if previous.is_some_and(|prev| block == prev + 1) {
                current_len += 1;
            } else {
                if current_len > best_len {
                    best_start = Some(current_start);
                    best_len = current_len;
                }
                current_start = block;
                current_len = 1;
            }
            previous = Some(block);

            if current_len >= max_blocks {
                best_start = Some(current_start);
                best_len = current_len;
                break;
            }
        }

        if current_len > best_len {
            best_start = Some(current_start);
            best_len = current_len;
        }

        let start = best_start?;
        let count = best_len.min(max_blocks);
        let blocks = (start..start + count as u64).collect::<Vec<_>>();
        for block in &blocks {
            self.free_blocks.remove(block);
        }
        Some(blocks)
    }
}

pub fn write_rows_to_scratch<R, W>(
    rows: &[Row],
    disk: &mut DiskClient<R, W>,
    scratch: &mut ScratchSpace,
) -> Result<ScratchRun>
where
    R: Read,
    W: Write,
{
    if rows.is_empty() {
        return Ok(ScratchRun::empty());
    }
    let mut writer = ScratchRunWriter::new(
        disk as *mut DiskClient<R, W>,
        scratch as *mut ScratchSpace,
        scratch.block_size(),
    );
    for row in rows {
        writer.push_row(row)?;
    }
    writer.finish()
}

pub fn write_rows_to_scratch_ptr<R, W>(
    rows: &[Row],
    disk: *mut DiskClient<R, W>,
    scratch: *mut ScratchSpace,
) -> Result<ScratchRun>
where
    R: Read,
    W: Write,
{
    unsafe { write_rows_to_scratch(rows, &mut *disk, &mut *scratch) }
}

pub fn read_scratch_run<R, W, F>(
    run: &ScratchRun,
    schema: &Schema,
    disk: &mut DiskClient<R, W>,
    sink: &mut F,
) -> Result<()>
where
    R: Read,
    W: Write,
    F: FnMut(Row) -> Result<()> + ?Sized,
{
    let block_size = disk.get_block_size()? as usize;
    let mut offset = 0usize;
    while offset < run.block_ids().len() {
        let batch_blocks =
            contiguous_batch_len(run.block_ids(), offset, SCRATCH_STREAM_BATCH_BLOCKS);
        let start_block = run.block_ids()[offset];
        let blocks = disk.read_blocks(start_block, batch_blocks as u64)?;
        for block_index in 0..batch_blocks {
            let start = block_index * block_size;
            let end = start + block_size;
            for row in decode_rows_from_block(&blocks[start..end], schema)? {
                sink(row)?;
            }
        }
        offset += batch_blocks;
    }
    Ok(())
}

pub fn read_scratch_run_ptr<R, W, F>(
    run: &ScratchRun,
    schema: &Schema,
    disk: *mut DiskClient<R, W>,
    sink: &mut F,
) -> Result<()>
where
    R: Read,
    W: Write,
    F: FnMut(Row) -> Result<()> + ?Sized,
{
    unsafe { read_scratch_run(run, schema, &mut *disk, sink) }
}

fn finalize_block(block: &mut [u8], count: u16) {
    let offset = block.len() - 2;
    block[offset..].copy_from_slice(&count.to_le_bytes());
}

#[inline]
fn contiguous_batch_len(block_ids: &[u64], start_index: usize, max_blocks: usize) -> usize {
    let mut batch_len = 1usize;
    while start_index + batch_len < block_ids.len()
        && batch_len < max_blocks
        && block_ids[start_index + batch_len] == block_ids[start_index + batch_len - 1] + 1
    {
        batch_len += 1;
    }
    batch_len
}
