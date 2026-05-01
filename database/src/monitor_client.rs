use anyhow::{Context, Result, bail};
use common::query::Query;
use std::io::{BufRead, BufReader, Read, Write};

use crate::row::Row;

pub struct MonitorClient<R: Read, W: Write> {
    reader: BufReader<R>,
    writer: W,
    row_buffer: Vec<u8>,
}

impl<R: Read, W: Write> MonitorClient<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            row_buffer: Vec::with_capacity(256),
        }
    }

    pub fn read_query(&mut self) -> Result<Query> {
        let mut line = String::new();
        self.reader.read_line(&mut line).context("read query")?;
        if line.trim().is_empty() {
            bail!("empty query");
        }
        serde_json::from_str(&line).context("parse query JSON")
    }

    pub fn get_memory_limit_mb(&mut self) -> Result<u32> {
        self.writer
            .write_all(b"get_memory_limit\n")
            .context("write")?;
        self.writer.flush().context("flush")?;
        let mut line = String::new();
        self.reader.read_line(&mut line).context("read")?;
        line.trim().parse().context("parse memory limit")
    }

    pub fn begin_validation(&mut self) -> Result<()> {
        self.writer.write_all(b"validate\n").context("write")?;
        self.writer.flush().context("flush")?;
        Ok(())
    }

    pub fn send_row(&mut self, row: &Row) -> Result<()> {
        self.row_buffer.clear();
        for value in row.values() {
            append_formatted_value(&mut self.row_buffer, value)?;
            self.row_buffer.push(b'|');
        }
        self.row_buffer.push(b'\n');
        self.writer
            .write_all(&self.row_buffer)
            .context("write formatted row")?;
        Ok(())
    }

    pub fn finish_validation(&mut self) -> Result<()> {
        self.writer.write_all(b"!\n").context("write")?;
        self.writer.flush().context("flush")?;
        Ok(())
    }
}

fn append_formatted_value(buf: &mut Vec<u8>, value: &common::Data) -> Result<()> {
    match value {
        common::Data::Int32(v) => write!(buf, "{v}").context("format Int32")?,
        common::Data::Int64(v) => write!(buf, "{v}").context("format Int64")?,
        common::Data::Float32(v) => write!(buf, "{v:?}").context("format Float32")?,
        common::Data::Float64(v) => write!(buf, "{v:?}").context("format Float64")?,
        common::Data::String(v) => buf.extend_from_slice(v.as_bytes()),
    }
    Ok(())
}
