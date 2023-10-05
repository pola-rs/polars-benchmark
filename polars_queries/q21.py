import polars as pl

from polars_queries import utils

Q_NUM = 21


def q():
    line_item_ds = utils.get_line_item_ds()
    supplier_ds = utils.get_supplier_ds()
    nation_ds = utils.get_nation_ds()
    orders_ds = utils.get_orders_ds()

    var_1 = "SAUDI ARABIA"

    res_1 = (
        (
            line_item_ds.group_by("l_orderkey")
            .agg(pl.col("l_suppkey").n_unique().alias("nunique_col"))
            .filter(pl.col("nunique_col") > 1)
            .join(
                line_item_ds.filter(pl.col("l_receiptdate") > pl.col("l_commitdate")),
                on="l_orderkey",
            )
        )
    ).cache()

    q_final = (
        res_1.group_by("l_orderkey")
        .agg(pl.col("l_suppkey").n_unique().alias("nunique_col"))
        .join(res_1, on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("nunique_col") == 1)
        .filter(pl.col("n_name") == var_1)
        .filter(pl.col("o_orderstatus") == "F")
        .group_by("s_name")
        .agg(pl.count().alias("numwait"))
        .sort(by=["numwait", "s_name"], descending=[True, False])
        .limit(100)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
