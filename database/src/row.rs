use anyhow::{Result, anyhow};
use common::{Data, DataType};
use db_config::table::TableSpec;
use std::collections::HashMap;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<SchemaColumn>,
    name_to_index: HashMap<String, usize>,
}

impl Schema {
    pub fn from_table_spec(spec: &TableSpec) -> Self {
        Self::from_columns(
            spec.column_specs
                .iter()
                .map(|c| SchemaColumn {
                    name: c.column_name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect(),
        )
    }

    pub fn columns(&self) -> &[SchemaColumn] {
        &self.columns
    }
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    pub fn column_at(&self, index: usize) -> Option<&SchemaColumn> {
        self.columns.get(index)
    }

    pub fn project(&self, map: &[(String, String)]) -> Result<(Schema, Vec<usize>)> {
        let mut columns = Vec::with_capacity(map.len());
        let mut indexes = Vec::with_capacity(map.len());
        for (from, to) in map {
            let idx = self
                .index_of(from)
                .ok_or_else(|| anyhow!("Unknown column: {from}"))?;
            let col = &self.columns[idx];
            columns.push(SchemaColumn {
                name: to.clone(),
                data_type: col.data_type.clone(),
            });
            indexes.push(idx);
        }
        Ok((Schema::from_columns(columns), indexes))
    }

    pub fn combine(left: &Schema, right: &Schema) -> Schema {
        let mut columns = Vec::with_capacity(left.len() + right.len());
        columns.extend_from_slice(left.columns());
        columns.extend_from_slice(right.columns());
        Schema::from_columns(columns)
    }

    fn from_columns(columns: Vec<SchemaColumn>) -> Self {
        let mut name_to_index = HashMap::with_capacity(columns.len());
        for (index, column) in columns.iter().enumerate() {
            name_to_index.entry(column.name.clone()).or_insert(index);
        }
        Self {
            columns,
            name_to_index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<Data>,
}

impl Row {
    pub fn new(values: Vec<Data>) -> Self {
        Self { values }
    }
    pub fn values(&self) -> &[Data] {
        &self.values
    }
    pub fn get(&self, index: usize) -> Option<&Data> {
        self.values.get(index)
    }

    pub fn into_project(self, indexes: &[usize]) -> Result<Row> {
        let mut remaining = vec![0usize; self.values.len()];
        for &index in indexes {
            *remaining
                .get_mut(index)
                .ok_or_else(|| anyhow!("bad index {index}"))? += 1;
        }

        let mut source = self.values.into_iter().map(Some).collect::<Vec<_>>();
        let mut values = Vec::with_capacity(indexes.len());
        for &index in indexes {
            let left = remaining
                .get_mut(index)
                .ok_or_else(|| anyhow!("bad index {index}"))?;
            let slot = source
                .get_mut(index)
                .ok_or_else(|| anyhow!("bad index {index}"))?;
            if *left == 1 {
                values.push(slot.take().ok_or_else(|| anyhow!("bad index {index}"))?);
            } else {
                values.push(
                    slot.as_ref()
                        .ok_or_else(|| anyhow!("bad index {index}"))?
                        .clone(),
                );
                *left -= 1;
            }
        }

        Ok(Row::new(values))
    }

    pub fn combine(left: &Row, right: &Row) -> Row {
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend_from_slice(&left.values);
        values.extend_from_slice(&right.values);
        Row::new(values)
    }

    pub fn serialized_size(&self) -> usize {
        self.values.iter().map(serialized_size_of).sum()
    }

    pub fn estimated_heap_bytes(&self) -> usize {
        size_of::<Self>()
            + self.values.capacity() * size_of::<Data>()
            + self.values.iter().map(heap_bytes_of).sum::<usize>()
    }

    pub fn encode_into(&self, buf: &mut [u8]) -> Result<()> {
        let mut offset = 0;
        for value in &self.values {
            offset += encode_data(value, &mut buf[offset..])?;
        }
        Ok(())
    }
}

fn serialized_size_of(value: &Data) -> usize {
    match value {
        Data::Int32(_) => 4,
        Data::Int64(_) => 8,
        Data::Float32(_) => 4,
        Data::Float64(_) => 8,
        Data::String(s) => s.len() + 1,
    }
}

fn heap_bytes_of(value: &Data) -> usize {
    match value {
        Data::String(s) => s.capacity(),
        _ => 0,
    }
}

pub fn encode_data(value: &Data, buf: &mut [u8]) -> Result<usize> {
    match value {
        Data::Int32(v) => {
            buf[..4].copy_from_slice(&v.to_le_bytes());
            Ok(4)
        }
        Data::Int64(v) => {
            buf[..8].copy_from_slice(&v.to_le_bytes());
            Ok(8)
        }
        Data::Float32(v) => {
            buf[..4].copy_from_slice(&v.to_le_bytes());
            Ok(4)
        }
        Data::Float64(v) => {
            buf[..8].copy_from_slice(&v.to_le_bytes());
            Ok(8)
        }
        Data::String(s) => {
            let b = s.as_bytes();
            buf[..b.len()].copy_from_slice(b);
            buf[b.len()] = 0;
            Ok(b.len() + 1)
        }
    }
}
