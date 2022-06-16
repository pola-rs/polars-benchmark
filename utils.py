import os

INCLUDE_IO = bool(os.environ.get("INCLUDE_IO", False))
SHOW_RESULTS = bool(os.environ.get("SHOW_RESULTS", False))
LOG_TIMINGS = bool(os.environ.get("LOG_TIMINGS", False))
print("include io:", INCLUDE_IO)
print("show results:", INCLUDE_IO)
print("log timings:", LOG_TIMINGS)

CWD = os.path.dirname(os.path.realpath(__file__))
DATASET_BASE_DIR = os.path.join(CWD, "tables_scale_1")
ANSWERS_BASE_DIR = os.path.join(CWD, "tpch-dbgen/answers")
TIMINGS_FILE = os.path.join(CWD, "timings.csv")


def append_row(solution: str, q: str, secs: float):
    with open(TIMINGS_FILE, "a") as f:
        if f.tell() == 0:
            f.write("solution,query_no,duration[s],include_io\n")
        f.write(f"{solution},{q},{secs},{INCLUDE_IO}\n")


def on_second_call(func):
    def helper(*args, **kwargs):
        helper.calls += 1

        # first call is outside the function
        # this call must set the result
        if helper.calls == 1:
            # include IO will compute the result on the 2nd call
            if not INCLUDE_IO:
                helper.result = func(*args, **kwargs)
            return helper.result

        # second call is in the query, now we set the result
        if INCLUDE_IO and helper.calls == 2:
            helper.result = func(*args, **kwargs)

        return helper.result

    helper.calls = 0
    helper.result = None

    return helper
