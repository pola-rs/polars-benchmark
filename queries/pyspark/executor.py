from linetimer import CodeTimer

# TODO: works for now, but need dynamic imports for this.
from queries.pyspark import (  # noqa: F401
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

    with CodeTimer(name="Overall execution of ALL spark queries", unit="s"):
        for query_number in range(1, num_queries + 1):
            submodule = f"q{query_number}"
            try:
                eval(f"{submodule}.q()")
            except Exception as exc:
                print(
                    f"Exception occurred while executing PySpark query {query_number}:\n{exc}"
                )
