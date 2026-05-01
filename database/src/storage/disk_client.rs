use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Read, Write};

pub struct DiskClient<R: Read, W: Write> {
    reader: BufReader<R>,
    writer: W,
    block_size: Option<u64>,
}

impl<R: Read, W: Write> DiskClient<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            block_size: None,
        }
    }

    pub fn get_block_size(&mut self) -> Result<u64> {
        if let Some(bs) = self.block_size {
            return Ok(bs);
        }
        let bs = self.cmd_u64("get block-size\n")?;
        self.block_size = Some(bs);
        Ok(bs)
    }

    pub fn get_file_start_block(&mut self, file_id: &str) -> Result<u64> {
        self.cmd_u64(&format!("get file start-block {file_id}\n"))
    }

    pub fn get_file_num_blocks(&mut self, file_id: &str) -> Result<u64> {
        self.cmd_u64(&format!("get file num-blocks {file_id}\n"))
    }

    pub fn get_anon_start_block(&mut self) -> Result<u64> {
        self.cmd_u64("get anon-start-block\n")
    }

    pub fn read_blocks(&mut self, start: u64, count: u64) -> Result<Vec<u8>> {
        let bs = self.get_block_size()? as usize;
        let cmd = format!("get block {start} {count}\n");
        self.writer
            .write_all(cmd.as_bytes())
            .context("write read cmd")?;
        self.writer.flush().context("flush read cmd")?;
        let mut buf = vec![0u8; bs * count as usize];
        self.reader
            .read_exact(&mut buf)
            .context("read block data")?;
        Ok(buf)
    }

    pub fn write_blocks(&mut self, start_block_id: u64, count: u64, data: &[u8]) -> Result<()> {
        let bs = self.get_block_size()? as usize;
        let expected_len = bs
            .checked_mul(count as usize)
            .context("scratch write size overflow")?;
        if data.len() != expected_len {
            anyhow::bail!(
                "scratch write size mismatch: expected {} bytes for {} blocks, got {} bytes",
                expected_len,
                count,
                data.len()
            );
        }

        let cmd = format!("put block {start_block_id} {count}\n");
        self.writer
            .write_all(cmd.as_bytes())
            .context("write put cmd")?;
        self.writer.write_all(data).context("write block data")?;
        self.writer.flush().context("flush put cmd")?;
        Ok(())
    }

    fn cmd_u64(&mut self, cmd: &str) -> Result<u64> {
        self.writer.write_all(cmd.as_bytes()).context("write cmd")?;
        self.writer.flush().context("flush cmd")?;
        let mut line = String::new();
        self.reader.read_line(&mut line).context("read response")?;
        line.trim()
            .parse()
            .with_context(|| format!("parse u64: '{}'", line.trim()))
    }
}
