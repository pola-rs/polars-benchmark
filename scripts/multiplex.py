import time

import polars as pl

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

pl.Config.set_engine_affinity(engine="streaming")

queries = [
    q1(),
    q2(),
    q3(),
    q4(),
    q5(),
    q6(),
    q7(),
    q8(),
    q9(),
    q10(),
    q11(),
    q12(),
    q13(),
    q14(),
    q15(),
    q16(),
    q17(),
    q18(),
    q19(),
    q20(),
    q21(),
    q22(),
]

t0 = time.time()
for lf in queries:
    print(lf.collect())
print(time.time() - t0)

t0 = time.time()
print(pl.collect_all(queries))
print(time.time() - t0)
