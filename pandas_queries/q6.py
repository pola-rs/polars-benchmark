from datetime import datetime

import pandas as pd
import pyarrow.compute as pc

from pandas_queries import utils

Q_NUM = 6


def q():
    date1 = datetime(1994, 1, 1)
    date2 = datetime(1995, 1, 1)
    var3 = 24
    columns = ["l_extendedprice", "l_discount"]
    kwargs_line = {
        "filters": (pc.field("l_shipdate") >= date1)
        & (pc.field("l_shipdate") < date2)
        & (pc.field("l_discount") >= 0.05)
        & (pc.field("l_discount") <= 0.07)
        & (pc.field("l_quantity") <= var3)
    }

    line_item_ds = utils.get_line_item_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds(columns=columns, kwargs=kwargs_line)

    def query():
        nonlocal line_item_ds
        flineitem = line_item_ds(columns=columns, kwargs=kwargs_line)
        result_value = (flineitem.l_extendedprice * flineitem.l_discount).sum()
        result_df = pd.DataFrame({"revenue": [result_value]})
        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
