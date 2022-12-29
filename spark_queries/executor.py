from linetimer import CodeTimer

if __name__ == "__main__":
    num_queries = 7

    with CodeTimer(name=f"Overall execution of ALL spark queries", unit="s"):
        sub_modules = [f"q{sm}" for sm in range(1, num_queries + 1)]
        for sm in sub_modules:
            try:
                eval(f"{sm}.q()")
            except:
                print(f"Exception occurred while executing spark_queries.{sm}")
