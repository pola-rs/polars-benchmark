import numpy as np
import pandas as pd
import vaex

from vaex_queries import utils

Q_NUM = 6


def q():
    date1 = np.datetime64("1994-01-01")
    date2 = np.datetime64("1995-01-01")
    var3 = 24

    line_item_ds = utils.get_line_item_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query():
        nonlocal line_item_ds
        line_item_ds = line_item_ds()

        lineitem_filtered = line_item_ds[
            ["l_quantity", "l_extendedprice", "l_discount", "l_shipdate"]
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
        result_df = pd.DataFrame({"revenue": [float(result_value)]})
        return vaex.from_pandas(result_df)

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
