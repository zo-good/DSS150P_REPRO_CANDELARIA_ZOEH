## Conclusions for this project

The numbers above come from one laptop and a small table (10,433 rows, about 7 MB as CSV),
so differences of a few milliseconds are noisy. The conclusions below are about **this**
workload only.

| Use | Best choice here | Why (from the measurements) |
|---|---|---|
| **Source exchange** | CSV | The original data arrived as CSV, and anyone can open it (Excel, any language, a text editor). It is about 4x larger than Parquet and has no types: dates and numbers must be re-parsed on every read. |
| **Pipeline intermediate storage** (staging, curated) | Parquet | Smallest file (1.9 MB vs 7.4 MB CSV), fastest write and read (about 9 ms vs 113 ms for CSV), and it keeps column types, so a date stays a date between steps. That removes a whole class of type bugs. |
| **Model input** | Parquet | `train.py` reads the whole curated table on every run. Parquet is the fastest full read and keeps the exact dtypes the features were built with, which supports reproducibility. |
| **Analytical querying** | PostgreSQL | A selective question (one city, one year) took about 3 ms, because the `(loc, date)` primary key is also an index. A file format must read everything and then filter. PostgreSQL also enforces the key and `CHECK` rules and allows many users at once. But a **full-table** read was the slowest (about 273 ms), so it is not the best for bulk model input. |

**JSONL** was the largest format (20.5 MB, about 2.8x CSV), because every row repeats every
column name. It suits nested or API-style records, not a flat 60-column table like this one.

**Partitioning by year** (the column that matches how the data is analysed, by season and
year) lets a reader load only `year=2019` without touching the other five years. The row
count check confirms the partition holds exactly the 2019 records. At 10k rows the time
saved is only milliseconds, but the benefit grows with the data: if the dataset grew 100x,
reading one year would still touch about 1/6 of the data.

**Limitations:** warm reads benefit from the operating system's file cache; PostgreSQL timings
include the Docker network hop on localhost; a wider, larger or nested dataset could rank
the formats differently.