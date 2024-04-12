from __future__ import annotations

from datetime import date

import modin.pandas as pd

from queries.modin import utils

Q_NUM = 6


def q() -> None:
    line_item_ds = utils.get_line_item_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        line_item_ds = line_item_ds()

        var1 = date(1994, 1, 1)
        var2 = date(1995, 1, 1)
        var3 = 0.05
        var4 = 0.07
        var5 = 24

        flineitem = line_item_ds[
            (line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)
        ]
        flineitem = line_item_ds[
            (line_item_ds["l_discount"] >= var3) & (line_item_ds["l_discount"] <= var4)
        ]
        flineitem = line_item_ds[line_item_ds["l_quantity"] < var5]
        result_value = (flineitem["l_extendedprice"] * flineitem["l_discount"]).sum()
        result_df = pd.DataFrame({"revenue": [result_value]})

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
