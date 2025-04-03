import polars as pl

from queries.polars import utils

Q_NUM = 2


def q(
    customer: None | pl.LazyFrame,
    lineitem: None | pl.LazyFrame,
    nation: None | pl.LazyFrame,
    orders: None | pl.LazyFrame,
    partsupp: None | pl.LazyFrame,
    supplier: None | pl.LazyFrame,
    region: None | pl.LazyFrame,
    part: None | pl.LazyFrame,
    **kwargs


) -> pl.LazyFrame:
    if nation is None:
        nation = utils.get_nation_ds()
        part = utils.get_part_ds()
        partsupp = utils.get_part_supp_ds()
        region = utils.get_region_ds()
        supplier = utils.get_supplier_ds()

    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    q1 = (
        part.join(partsupp, left_on="p_partkey", right_on="ps_partkey")
        .join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .join(region, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("p_size") == var1)
        .filter(pl.col("p_type").str.ends_with(var2))
        .filter(pl.col("r_name") == var3)
    )

    return (
        q1.group_by("p_partkey")
        .agg(pl.min("ps_supplycost"))
        .join(q1, on=["p_partkey", "ps_supplycost"])
        .select(
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        )
        .sort(
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            descending=[True, False, False, False],
        )
        .head(100)
    )


if __name__ == "__main__":
    utils.run_query(Q_NUM, q())
