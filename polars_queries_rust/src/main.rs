use chrono::NaiveDateTime;
use jemallocator::Jemalloc;
use polars::error::PolarsError;
use polars::prelude::PolarsResult;

mod q1;
mod q21;
mod utils;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

fn main() -> PolarsResult<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let q_no = args[1]
        .parse::<u8>()
        .map_err(|_| PolarsError::ComputeError("query no. not given/understood".into()))?;

    let q = match q_no {
        1 => q1::query(),
        21 => q21::query(),
        q => Err(PolarsError::ComputeError(
            format!("query {q} does not exist").into(),
        )),
    }?;

    let out = q.with_comm_subplan_elim(true).collect()?;
    dbg!(out);
    Ok(())
}
