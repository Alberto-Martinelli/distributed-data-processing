# Hadoop and Spark Labs

Java exercises from the **Big Data Processing and Analytics** course at **Politecnico di Torino**, working with text, Amazon reviews, and Barcelona bike-sharing data.

- **Hadoop (Labs 1–4):** word and bigram counts, filtering with counters, top-100 product pairs, and rating normalization. Uses map-only jobs, custom `Writable` types, and chained MapReduce jobs.
- **Spark RDDs (Labs 5–7):** word-frequency analysis, products reviewed together, and bike-station availability. Uses grouping, deduplication, `reduceByKey`, joins, and ranking.
- **Spark SQL (Lab 8):** bike-station analysis written with DataFrames, typed Datasets, and SQL, including schemas, UDFs, and grouped aggregations.

The code follows the course templates, with a few bug fixes and updated dependencies.

## Running

Requires Java 17 and Maven, plus Spark 3.5.3 or Hadoop 3.3.4 for the relevant labs.

Build everything and run Lab 6 on its sample data:

```sh
mvn package
./Lab6/runLocal.sh
```

Each lab has `runLocal.sh` and `run.sh` for local and YARN/HDFS execution. They accept custom input/output arguments; output directories must not already exist.
