import os

INCLUDE_IO = bool(os.environ.get("INCLUDE_IO", False))
print("include io:", INCLUDE_IO)

CWD = os.path.dirname(os.path.realpath(__file__))
__default_dataset_base_dir = os.path.join(CWD, "tables_scale_1")
__default_answers_base_dir = os.path.join(CWD, "tpch-dbgen/answers")


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
