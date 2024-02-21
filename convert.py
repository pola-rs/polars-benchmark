from pathlib import Path

import polars as pl


def get_query_answer(query: int, base_dir: str = "tpch-dbgen/answers") -> pl.LazyFrame:
    answer_ldf = pl.scan_csv(
        Path(base_dir) / f"q{query}.out",
        separator="|",
        has_header=True,
        try_parse_dates=True,
    )
    cols = answer_ldf.columns
    answer_ldf = answer_ldf.select(
        [pl.col(c).alias(c.strip()) for c in cols]
    ).with_columns([pl.col(pl.datatypes.Utf8).str.strip_chars().name.keep()])

    return answer_ldf


for i in range(1, 23):
    df = get_query_answer(i).collect()
    out = f"data/answers/q{i}.parquet"
    df.write_parquet(out)
