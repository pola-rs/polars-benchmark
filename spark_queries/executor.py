from linetimer import CodeTimer

# TODO: works for now, but need dynamic imports for this.
from spark_queries import (
    q1,
    q2,
    q3,
    q4,
    q5,
    q6,
    q7,
    q8,
    q9,
    q10,
    q11,
    q12,
    q13,
    q14,
    q15,
    q16,
    q17,
    q18,
    q19,
    q20,
    q21,
    q22,
)

if __name__ == "__main__":
    num_queries = 22

    with CodeTimer(name=f"Overall execution of ALL spark queries", unit="s"):
        sub_modules = [f"q{sm}" for sm in range(1, num_queries + 1)]
        for sm in sub_modules:
            try:
                eval(f"{sm}.q()")
            except:
                print(f"Exception occurred while executing spark_queries.{sm}")
