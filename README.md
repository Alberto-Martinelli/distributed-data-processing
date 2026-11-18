# Distributed Data Processing with Hadoop and Spark

Course project for **Big Data Processing and Analytics** at **Politecnico di Torino**.

A collection of Java exercises covering distributed batch processing with **Hadoop MapReduce** and **Apache Spark**. The labs progress from word counts and filtering to product co-occurrence, rating normalization, and bike-sharing station analysis.

## Apache Spark

- **RDD transformations and actions:** `filter`, `map`, `mapToPair`, `flatMapToPair`, `mapValues`, `fold`, `count`, and `take` to build and execute analytical pipelines.
- **Grouping and aggregation:** `groupByKey` to assemble each user's reviewed products, duplicate removal within each group, and `reduceByKey` to count product pairs and combine station readings.
- **Ranking:** `sortByKey(false)` followed by `take(10)` to retrieve the most frequently co-reviewed product pairs.
- **Joins and time-based analysis:** aggregate bike-station readings by station, weekday, and hour; calculate criticality; select each station's most critical timeslot; and join station coordinates.
- **DataFrames and typed Datasets:** CSV ingestion, explicit station schemas, column expressions, grouped averages, joins, sorting, and Java bean encoders with typed transformations.
- **Spark SQL:** temporary views, user-defined functions, `GROUP BY`, `HAVING`, joins, and multi-column `ORDER BY` for the structured bike-sharing analysis.
- **Output and execution:** text and CSV output, KML placemark generation, `coalesce(1)` for a single output partition in Lab 7, and `spark-submit` launchers for local execution or YARN.

## Hadoop MapReduce

- **Key/value processing:** mapper and reducer implementations for word counts, bigrams, product pairs, and numerical aggregations.
- **Map-only filtering:** a configurable prefix filter with zero reducers, avoiding an unnecessary shuffle.
- **Job counters:** track selected and discarded records and retrieve totals in the driver.
- **Chained jobs:** use one job's output as the next job's input for global top-k selection and user-centered rating normalization.
- **Custom serialization:** `Writable` records carry product/rating and product-pair/count values; `Comparable` defines ranking order for top-k selection.
- **Job configuration:** `ToolRunner`, configurable reducer counts, `TextInputFormat`, `KeyValueTextInputFormat`, and launchers for local execution or HDFS/YARN.

## Exercises and patterns

| Exercise | Analysis | Main patterns |
| --- | --- | --- |
| [Lab1](Lab1) / [Lab1Bonus](Lab1Bonus) | Word and bigram frequencies | Tokenization, key/value emission, counting |
| [Lab2](Lab2) | Word-frequency filtering | Map-only job, configurable prefix, counters |
| [Lab3](Lab3) | Top-100 co-occurring product pairs | Pair generation, distributed counting, bounded top-k in a final reducer |
| [Lab4](Lab4) | Product ratings adjusted for user rating habits | Group by user, subtract user mean, regroup and average by product |
| [Lab5](Lab5) | Frequent words matching a prefix | RDD filtering, maximum via `fold`, threshold relative to the maximum |
| [Lab6](Lab6) | Products frequently reviewed by the same users | Grouping, deduplication, pair generation, frequency aggregation, ranking |
| [Lab7](Lab7) | Most critical bike-sharing timeslot per station | Invalid-reading filtering, temporal aggregation, ratio calculation, deterministic tie-breaking, join |
| [Lab8_DataFrame](Lab8_DataFrame) / [Lab8_Dataset](Lab8_Dataset) / [Lab8_SQL](Lab8_SQL) | Bike-station timeslots above a criticality threshold | The same analysis through DataFrames, typed Datasets, and SQL |

The collection shows related problems across different processing models: product co-occurrence with MapReduce and Spark RDDs, and bike-station criticality with RDDs and structured Spark APIs. Lab 7 selects one timeslot per station; the Lab 8 variants return all timeslots above the threshold.

## Run

Requires **Java 17**, **Maven**, **Spark 3.5.3** for Spark exercises, and **Hadoop 3.3.4** for Hadoop exercises.

Build all modules:

```sh
mvn package
```

Run a Spark exercise on its included sample data:

```sh
./Lab6/runLocal.sh
```

Each lab includes `runLocal.sh` and `run.sh` for local and YARN/HDFS execution. Both accept custom input/output arguments. Cluster inputs must already be on HDFS, and output directories must not already exist.

## Course context

These exercises are based on course templates and utilities. Lab1Bonus and the three Lab 8 variants were copied from the solution folders in the original course materials. The original code structure is preserved, with targeted correctness fixes and updated dependencies.
