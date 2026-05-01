use anyhow::{Context, Result, bail};
use common::{Data, DataType};

use crate::row::{Row, Schema};

pub fn decode_rows_from_block(block: &[u8], schema: &Schema) -> Result<Vec<Row>> {
    if block.len() < 2 {
        bail!("block too short");
    }
    let data_limit = block.len() - 2;
    let row_count = u16::from_le_bytes([block[data_limit], block[data_limit + 1]]) as usize;
    let mut offset = 0;
    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        let mut values = Vec::with_capacity(schema.len());
        for col in schema.columns() {
            let (value, consumed) =
                decode_value_from_bytes(&block[offset..data_limit], &col.data_type)
                    .with_context(|| format!("decode col {}", col.name))?;
            offset += consumed;
            values.push(value);
        }
        rows.push(Row::new(values));
    }

    Ok(rows)
}

pub fn decode_projected_rows_from_block(
    block: &[u8],
    schema: &Schema,
    indexes: &[usize],
) -> Result<Vec<Row>> {
    if block.len() < 2 {
        bail!("block too short");
    }

    let wanted = projected_positions_by_column(schema.len(), indexes)?;
    let data_limit = block.len() - 2;
    let row_count = u16::from_le_bytes([block[data_limit], block[data_limit + 1]]) as usize;
    let mut offset = 0;
    let mut rows = Vec::with_capacity(row_count);

    for _ in 0..row_count {
        let mut values = vec![None; indexes.len()];
        for (column_index, col) in schema.columns().iter().enumerate() {
            if wanted[column_index].is_empty() {
                offset += skip_value_from_bytes(&block[offset..data_limit], &col.data_type)
                    .with_context(|| format!("skip col {}", col.name))?;
                continue;
            }

            let (value, consumed) =
                decode_value_from_bytes(&block[offset..data_limit], &col.data_type)
                    .with_context(|| format!("decode col {}", col.name))?;
            offset += consumed;
            for &position in &wanted[column_index] {
                values[position] = Some(value.clone());
            }
        }

        rows.push(Row::new(
            values
                .into_iter()
                .map(|value| value.context("projected column was not decoded"))
                .collect::<Result<Vec<_>>>()?,
        ));
    }

    Ok(rows)
}

fn projected_positions_by_column(
    column_count: usize,
    indexes: &[usize],
) -> Result<Vec<Vec<usize>>> {
    let mut wanted = vec![Vec::new(); column_count];
    for (position, &column_index) in indexes.iter().enumerate() {
        let Some(positions) = wanted.get_mut(column_index) else {
            bail!("projection index {column_index} out of bounds");
        };
        positions.push(position);
    }
    Ok(wanted)
}

pub fn decode_row_from_bytes(buf: &[u8], schema: &Schema) -> Result<Row> {
    let mut offset = 0usize;
    let mut values = Vec::with_capacity(schema.len());
    for column in schema.columns() {
        let (value, consumed) = decode_value_from_bytes(&buf[offset..], &column.data_type)
            .with_context(|| format!("decode col {}", column.name))?;
        offset += consumed;
        values.push(value);
    }
    Ok(Row::new(values))
}

pub fn decode_value_from_bytes(buf: &[u8], dt: &DataType) -> Result<(Data, usize)> {
    match dt {
        DataType::Int32 => {
            let b = buf.get(..4).context("short Int32")?;
            Ok((Data::Int32(i32::from_le_bytes([b[0], b[1], b[2], b[3]])), 4))
        }
        DataType::Int64 => {
            let b = buf.get(..8).context("short Int64")?;
            Ok((
                Data::Int64(i64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])),
                8,
            ))
        }
        DataType::Float32 => {
            let b = buf.get(..4).context("short Float32")?;
            Ok((
                Data::Float32(f32::from_le_bytes([b[0], b[1], b[2], b[3]])),
                4,
            ))
        }
        DataType::Float64 => {
            let b = buf.get(..8).context("short Float64")?;
            Ok((
                Data::Float64(f64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])),
                8,
            ))
        }
        DataType::String => {
            let zero = buf
                .iter()
                .position(|&b| b == 0)
                .context("no null terminator")?;
            let s = std::str::from_utf8(&buf[..zero])
                .context("invalid UTF-8")?
                .to_owned();
            Ok((Data::String(s), zero + 1))
        }
    }
}

pub fn skip_value_from_bytes(buf: &[u8], dt: &DataType) -> Result<usize> {
    match dt {
        DataType::Int32 | DataType::Float32 => {
            buf.get(..4).context("short 4-byte value")?;
            Ok(4)
        }
        DataType::Int64 | DataType::Float64 => {
            buf.get(..8).context("short 8-byte value")?;
            Ok(8)
        }
        DataType::String => Ok(buf
            .iter()
            .position(|&b| b == 0)
            .context("no null terminator")?
            + 1),
    }
}
