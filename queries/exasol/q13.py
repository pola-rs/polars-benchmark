from queries.exasol import utils

Q_NUM = 13


def q() -> None:

    customer_ds = utils.get_customer_ds()
    orders_ds = utils.get_orders_ds()
    query_str = f"""
    select
            c_count, count(*) as custdist
        from (
        select
            c_custkey,
            count(o_orderkey) as c_count
            from
                {customer_ds} left outer join {orders_ds} on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
            group by
                c_custkey
            ) c_orders
        group by
            c_count
        order by
            custdist desc,
            c_count desc
        ;
    """
    utils.run_query(Q_NUM, query_str)


if __name__ == "__main__":
    q()

