from queries.exasol import utils

Q_NUM = 15


def q() -> None:

    supplier_ds = utils.get_supplier_ds()
    line_item_ds = utils.get_line_item_ds()
    query_str = f"""
    WITH revenue AS (
        SELECT
            l_suppkey AS supplier_no,
            SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM {line_item_ds}
        WHERE l_shipdate >= DATE '1996-01-01'
          AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
        GROUP BY l_suppkey
    )
    SELECT
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
    FROM {supplier_ds}, revenue
    WHERE s_suppkey = supplier_no
      AND total_revenue = (
          SELECT MAX(total_revenue) FROM revenue
      )
    ORDER BY s_suppkey
    ;
    """
    utils.run_query(Q_NUM, query_str)


if __name__ == "__main__":
    q()

