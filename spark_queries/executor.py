from linetimer import CodeTimer

# TODO: works for now, but need dynamic imports for this.
from spark_queries import q1, q2, q3, q4, q5, q6, q7

if __name__ == "__main__":
    num_queries = 7

    with CodeTimer(name=f"Overall execution of ALL spark queries", unit="s"):
        sub_modules = [f"q{sm}" for sm in range(1, num_queries + 1)]
        for sm in sub_modules:
            try:
                eval(f"{sm}.q()")
            except:
                print(f"Exception occurred while executing spark_queries.{sm}")
