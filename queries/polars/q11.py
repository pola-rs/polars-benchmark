import polars as pl

from queries.polars import utils

Q_NUM = 11


def q() -> None:
    supplier = utils.get_supplier_ds()
    partsupp = utils.get_part_supp_ds()
    nation = utils.get_nation_ds()

    var1 = "GERMANY"
    var2 = 0.0001

    q1 = (
        partsupp.join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("n_name") == var1)
    )
    q2 = q1.select(
        (pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().round(2).alias("tmp")
        * var2
    ).with_columns(pl.lit(1).alias("lit"))

    q_final = (
        q1.group_by("ps_partkey")
        .agg(
            (pl.col("ps_supplycost") * pl.col("ps_availqty"))
            .sum()
            .round(2)
            .alias("value")
        )
        .with_columns(pl.lit(1).alias("lit"))
        .join(q2, on="lit")
        .filter(pl.col("value") > pl.col("tmp"))
        .select("ps_partkey", "value")
        .sort("value", descending=True)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
