use crate::utils::*;
use chrono::NaiveDate;
use polars::prelude::*;

// def q():
// line_item_ds = utils.get_line_item_ds()
// supplier_ds = utils.get_supplier_ds()
// nation_ds = utils.get_nation_ds()
// orders_ds = utils.get_orders_ds()
//
// var_1 = "SAUDI ARABIA"
//
// res_1 = (
// (
// line_item_ds.group_by("l_orderkey")
// .agg(pl.col("l_suppkey").n_unique().alias("nunique_col"))
// .filter(pl.col("nunique_col") > 1)
// .join(
// line_item_ds.filter(pl.col("l_receiptdate") > pl.col("l_commitdate")),
// on="l_orderkey",
// )
// )#.collect().lazy()
// )
//
// q_final = (
// res_1
// .group_by("l_orderkey")
// .agg(pl.col("l_suppkey").n_unique().alias("nunique_col"))
// .join(res_1, on="l_orderkey", allow_parallel=False)
// # .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey", allow_parallel=False)
// # .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey", allow_parallel=False)
// # .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey", allow_parallel=False)
// # .filter(pl.col("nunique_col") == 1)
// # .filter(pl.col("n_name") == var_1)
// # .filter(pl.col("o_orderstatus") == "F")
// # .group_by("s_name")
// # .agg(pl.count().alias("numwait"))
// # .sort(by=["numwait", "s_name"], descending=[True, False])
// .limit(100)
// )
//
// # q_final.profile(show_plot=True)
//
// print(q_final.explain())
// # print(q_final.collect())
// utils.run_query(Q_NUM, q_final)
//
//
// if __name__ == "__main__":
// q()

pub fn query() -> PolarsResult<LazyFrame> {
    let line_item_ds = get_lineitem_ds().slice(0, 1_000_000);

    let t0 = std::time::Instant::now();
    let res1 = line_item_ds
        .group_by([col("l_orderkey")])
        .agg([col("l_suppkey").n_unique().alias("nunique_col")])
        .filter(col("nunique_col").gt(lit(1)))
        .inner_join(
            get_lineitem_ds().filter(col("l_receiptdate").gt(col("l_commitdate"))),
            "l_orderkey",
            "l_orderkey",
        ).cache();

    let q = res1
        .clone()
        .group_by([col("l_orderkey")])
        .agg([col("l_suppkey").n_unique().alias("nunique_col")])
        .inner_join(res1,
                    "l_orderkey",
                    "l_orderkey",
        );
    q.collect();
    dbg!(t0.elapsed().as_millis());
    todo!();

    // let q = get_lineitem_ds();
    // let var_1 = lit(NaiveDate::from_ymd_opt(1998, 9, 2)
    //     .unwrap()
    //     .and_hms_opt(0, 0, 0)
    //     .unwrap());
    //
    // let q = q
    //     .filter(col("l_shipdate").lt_eq(var_1))
    //     .groupby([cols(["l_returnflag", "l_linestatus"])])
    //     .agg([
    //         sum("l_quantity").alias("sum_qty"),
    //         sum("l_extendedprice").alias("sum_base_price"),
    //         (col("l_extendedprice") * (lit(1) - col("l_discount")))
    //             .sum()
    //             .alias("sum_disc_price"),
    //         (col("l_extendedprice") * (lit(1.0) - col("l_discount")) * (lit(1.0) + col("l_tax")))
    //             .sum()
    //             .alias("sum_charge"),
    //         mean("l_quantity").alias("avg_qty"),
    //         mean("l_extendedprice").alias("avg_price"),
    //         mean("l_discount").alias("avg_disc"),
    //         count().alias("count_order"),
    //     ])
    //     .sort_by_exprs([cols(["l_returnflag", "l_linestatus"])], &[false], false);
    //
    // Ok(q)
}
