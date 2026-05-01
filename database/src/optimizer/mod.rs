use anyhow::Result;
use common::query::Query;
use db_config::DbContext;

mod rules;

pub fn optimize_query(query: &Query, ctx: &DbContext) -> Result<Query> {
    Ok(Query {
        root: rules::optimize_op(&query.root, ctx)?,
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use common::query::Query;
    use db_config::DbContext;

    use super::optimize_query;

    fn query_at(index: usize) -> anyhow::Result<Query> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../scratch/runtimes/tpch/monitor_config.json");
        let value: serde_json::Value = serde_json::from_str(&fs::read_to_string(root)?)?;
        Ok(serde_json::from_value(
            value["query_configs"][index]["query"].clone(),
        )?)
    }

    fn tpch_ctx() -> anyhow::Result<DbContext> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../scratch/runtimes/tpch/db_config.json");
        DbContext::load_from_file(&path)
    }

    #[test]
    #[ignore]
    fn dump_q54_plan() -> anyhow::Result<()> {
        let optimized = optimize_query(&query_at(53)?, &tpch_ctx()?)?;
        println!("{optimized:#?}");
        Ok(())
    }

    #[test]
    #[ignore]
    fn dump_q56_plan() -> anyhow::Result<()> {
        let optimized = optimize_query(&query_at(55)?, &tpch_ctx()?)?;
        println!("{optimized:#?}");
        Ok(())
    }

    #[test]
    #[ignore]
    fn dump_q58_plan() -> anyhow::Result<()> {
        let optimized = optimize_query(&query_at(57)?, &tpch_ctx()?)?;
        println!("{optimized:#?}");
        Ok(())
    }
}
