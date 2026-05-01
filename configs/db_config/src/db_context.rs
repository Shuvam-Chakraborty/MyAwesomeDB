use std::{collections::HashSet, fs, path::Path};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::table::TableSpec;

#[derive(Deserialize, Serialize, Debug)]
pub struct DbContext {
    table_specs: Vec<TableSpec>,
}

impl DbContext {
    pub fn from(table_specs: Vec<TableSpec>) -> Result<DbContext> {
        let db_context = DbContext { table_specs };
        Self::validate_context(&db_context)?;
        Ok(db_context)
    }

    pub fn load_from_file(context_config_path: &Path) -> Result<DbContext> {
        let context_file_contents = fs::read_to_string(context_config_path)?;

        let ctx: DbContext = serde_json::from_str(&context_file_contents)?;

        Self::validate_context(&ctx)?;

        Ok(ctx)
    }

    fn validate_context(ctx: &DbContext) -> Result<()> {
        let mut table_names = HashSet::new();
        let mut file_ids = HashSet::new();

        for table in &ctx.table_specs {
            if table.name.trim().is_empty() {
                bail!("table name cannot be empty");
            }
            if table.file_id.trim().is_empty() {
                bail!("table file_id cannot be empty");
            }
            if table.column_specs.is_empty() {
                bail!("table {} must define at least one column", table.name);
            }
            if !table_names.insert(table.name.clone()) {
                bail!("duplicate table name: {}", table.name);
            }
            if !file_ids.insert(table.file_id.clone()) {
                bail!("duplicate table file_id: {}", table.file_id);
            }

            let mut column_names = HashSet::new();
            for column in &table.column_specs {
                if column.column_name.trim().is_empty() {
                    bail!("table {} contains an empty column name", table.name);
                }
                if !column_names.insert(column.column_name.clone()) {
                    bail!(
                        "duplicate column name {} in table {}",
                        column.column_name,
                        table.name
                    );
                }
            }
        }

        Ok(())
    }

    pub fn table_specs(&self) -> &[TableSpec] {
        &self.table_specs
    }
}
