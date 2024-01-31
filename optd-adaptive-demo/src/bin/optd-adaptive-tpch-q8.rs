use std::collections::HashMap;
use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_optd_cli::exec::{exec_from_commands_collect, exec_from_files};
use datafusion_optd_cli::{
    exec::exec_from_commands,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
};
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use std::sync::Arc;
use std::time::Duration;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let session_config = SessionConfig::from_env()?.with_information_schema(true);

    let rn_config = RuntimeConfig::new();
    let runtime_env = RuntimeEnv::new(rn_config.clone())?;

    let mut ctx = {
        let mut state =
            SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
        let optimizer = DatafusionOptimizer::new_non_adaptive_optimizer(Box::new(DatafusionCatalog::new(
            state.catalog_list()
        )), HashMap::from([(String::from("part"), 200_000),
            (String::from("lineitem"), 6_000_000),
            (String::from("supplier"), 10_000),
            (String::from("customer"), 150_000),
            (String::from("partsupp"), 800_000),
            (String::from("nation"), 25),
            (String::from("orders"), 1_500_000),
            (String::from("region"), 5),
        ]));
        state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
        SessionContext::new_with_state(state)
    };
    ctx.refresh_catalogs().await?;

    let slient_print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: true,
        maxrows: MaxRows::Limited(5),
    };

    let print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
        maxrows: MaxRows::Limited(5),
    };

    exec_from_files(
        vec!["tpch/populate.sql".to_string()],
        &mut ctx,
        &slient_print_options,
    )
    .await;

    let mut iter = 0;

    loop {
        iter += 1;
        println!("--- ITERATION {} ---", iter);
        let sql = r#"
 SELECT s_suppkey FROM supplier, partsupp, nation WHERE s_suppkey = ps_suppkey and s_nationkey = n_nationkey;
        "#;
        let result = exec_from_commands_collect(&mut ctx, vec![format!("explain {}", sql)]).await?;
        println!(
            "{}",
            result
                .iter()
                .find(|x| x[0] == "physical_plan after optd-join-order")
                .map(|x| &x[1])
                .unwrap()
        );
        exec_from_commands(&mut ctx, &print_options, vec![sql.to_string()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
