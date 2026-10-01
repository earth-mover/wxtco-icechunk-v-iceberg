# Weather Forecast Data TCO Analysis

We need to produce a comprehensive study which compares three different methods of storing a dataset which originates as NetCDF or GRIB files

1. Flattening and transforming all of the data to Iceberg / Parquet
2. Writing a native Icechunk dataset with well-chosen chunking and compression
3. Creating the same-shaped dataset as a virtual Icechunk

We can think of this as an expanded version of [Tensors vs. Tables](https://www.earthmover.io/blog/tensors-vs-tables/#tensors-as-columns-vs-tensors-inside-tables) at real-world scale.

## Comparison Points

We need to compare the solutions on several dimensions:

- Storage cost: what is the total volume of data that gets stored at the end? This should have variants both with and without the original NetCDFs
- Query performance. We care about the speed of queries, and picking the queries heavily impacts the results of the comparison. I propose we start with two simple examples (assuming we use weather data).
    - Extraction of a forecast timeseries at a single point and computation of a simple derived quantity such as heating-degree days.
    - Regional ensemble mean statistics for each forecast (e.g. CRPS)
    - An AI-model-training dataloader pattern, where we’re many global variables from each forecast step into a GPU as fast as possible.
    - Latency of ETL
- Compute costs, which we bucket into two groups
    - ETL costs
    - Query costs. For this, we need some model of how frequently the queries are performed
    
    Compute costs must cover everything, such that we can truly get a picture of total cost of ownership (TCO).
    
There may be interesting tradeoffs we learn about along the way.

## Datasets: UK Met Office MOGREPS-G

Attributes of this dataset:
- Global ensemble weather forecast
- Stored in S3 in `eu-west-2`. Example file: s3://met-office-uk-ensemble-model-data/uk-ensemble/2026/08/31/T0000Z/20260831T0000Z-PT0000H00M-cloud_amount_of_total_cloud.nc
- Rolling archive (keeps 14 days of forecasts like)
- Relevant for current customers and prospects
- Big but not too big (100 TB)
- Already ingested into Earthmover virtually: https://app.earthmover.io/metoffice/mogreps-g. This derisks the feasibility of method 3.
    Note: we can't simply reuse this repo. We need to do everything from scratch.

## Deliverables

- Long-form blog post with comprehensive TCO analysis.
- Open-source library to reproduce ingestion and queries.

## Open Questions

### How much data to ingest?

The atomic unit of ingestion is a single forecast at a specific initialization time.
I propose that we restrict to:
- One week's worth of data (to avoid racing with the rolling updates / deletes)
- Only surface variables (to limit size)

### How to handle the rolling updates?

Any specific MOGREPS file will eventually disappear, making reproduction hard.
This will particularly impact the virtual dataset (option 3).

One good option would be to copy a subset of NetCDF files to a persistent location.

This would also allow us to move it to us-east-1, where Earthmover runs.

Even if building from a static copy, the ingestion should be orchestrated such that it is atomic to the individual forecast.
It's okay to run the backfill ingestion sequentially, since this is ultimately what the steady-date production workload looks like.

Out of scope: ingesting each forecast step as it is released. For now we can bulk ingest an entire forecast as soon as the full rollout is complete.

### Where to store the tables?

We can use the Arraylake Iceberg Catalog, already deployed in prod (but behind a feature flag)

### How to query the tables?

Options:
- Spark / AWS EMR
- Snowflake
- DuckDB
- Zax SQL

### How to query the tensors?

Options:
- Xarray
- Xarray + Dask
- Zax-SQL
- Zax-prototype
- Zax proper

### How to run and orchestrate ingestion?

Ingestion will have a significant cost. We will need to run Python (Xarray) to read the NetCDF files.
It needs to be in the cloud (ideally same region) to maximize throughput.

Options:
- AWS EC2
- AWS Lambda
- Coiled
- Modal
- Airflow

My preference is to limit the number of vendors. It would be great if we could just use AWS services
