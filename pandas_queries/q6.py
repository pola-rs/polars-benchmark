import datetime

import pandas as pd

from pandas_queries import pandas_tpch_utils

Q_NUM = 6


def q():
    date1 = datetime.datetime.strptime("1994-01-01", "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime("1995-01-01", "%Y-%m-%d").date()
    var3 = 24

    line_item_ds = pandas_tpch_utils.get_line_item_ds()

    def query():
        lineitem_filtered = line_item_ds.loc[
            :, ["l_quantity", "l_extendedprice", "l_discount", "l_shipdate"]
        ]
        sel = (
            (lineitem_filtered.l_shipdate >= date1)
            & (lineitem_filtered.l_shipdate < date2)
            & (lineitem_filtered.l_discount >= 0.05)
            & (lineitem_filtered.l_discount <= 0.07)
            & (lineitem_filtered.l_quantity < var3)
        )

        flineitem = lineitem_filtered[sel]
        result_value = (flineitem.l_extendedprice * flineitem.l_discount).sum()
        result_df = pd.DataFrame({"revenue": [result_value]})
        return result_df

    pandas_tpch_utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
