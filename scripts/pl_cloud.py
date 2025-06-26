import time

import polars as pl
import polars_cloud as pc

from queries.polars.q1 import q as q1
from queries.polars.q2 import q as q2
from queries.polars.q3 import q as q3
from queries.polars.q4 import q as q4
from queries.polars.q5 import q as q5
from queries.polars.q6 import q as q6
from queries.polars.q7 import q as q7
from queries.polars.q8 import q as q8
from queries.polars.q9 import q as q9
from queries.polars.q10 import q as q10
from queries.polars.q11 import q as q11
from queries.polars.q12 import q as q12
from queries.polars.q13 import q as q13
from queries.polars.q14 import q as q14
from queries.polars.q15 import q as q15
from queries.polars.q16 import q as q16
from queries.polars.q17 import q as q17
from queries.polars.q18 import q as q18
from queries.polars.q19 import q as q19
from queries.polars.q20 import q as q20
from queries.polars.q21 import q as q21
from queries.polars.q22 import q as q22

pc.login()


def _scan_ds(table_name: str) -> pl.LazyFrame:
    path = f"s3://polars-pdsh/scale-factor-100.0/200/{table_name}/"
    return pl.scan_parquet(path)


lineitem = _scan_ds("lineitem")
orders = _scan_ds("orders")
customer = _scan_ds("customer")
region = _scan_ds("region")
nation = _scan_ds("nation")
supplier = _scan_ds("supplier")
part = _scan_ds("part")
part_supp = _scan_ds("partsupp")

kwargs = {
    "lineitem": lineitem,
    "orders": orders,
    "customer": customer,
    "region": region,
    "nation": nation,
    "supplier": supplier,
    "part": part,
    "partsupp": part_supp,
}

queries = [
    q1(**kwargs),
    q2(**kwargs),
    q3(**kwargs),
    q4(**kwargs),
    q5(**kwargs),
    q6(**kwargs),
    q7(**kwargs),
    q8(**kwargs),
    q9(**kwargs),
    q10(**kwargs),
    q11(**kwargs),
    q12(**kwargs),
    q13(**kwargs),
    q14(**kwargs),
    q15(**kwargs),
    q16(**kwargs),
    q17(**kwargs),
    q18(**kwargs),
    q19(**kwargs),
    q20(**kwargs),
    q21(**kwargs),
    q22(**kwargs),
]


ctx = pc.ComputeContext(
    workspace="ritchie-workspace",
    instance_type="t3.xlarge",
    cluster_size=10,
    interactive=True,
)
ctx.start(wait=True)

print("started cluster")


timings: list[float | None] = []
for i, q in enumerate(queries):
    i += 1
    print(f"run q{i}")
    start_time = time.time()
    try:
        print(q.remote(ctx).distributed().show())
        execution_time = time.time() - start_time
        print(f"q{i} executed in: {execution_time:.2f} seconds")
        timings.append(execution_time)
    except pl.exceptions.ComputeError:
        timings.append(None)

print("timings: ", timings)

ctx.stop(wait=True)
