#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- sampleData/registerSample.csv sampleData/stations.csv 0.4 output
fi

spark-submit --class it.polito.bigdata.spark.example.SparkDriver --master yarn --deploy-mode client target/Lab8_SQL-1.0.0.jar "$@"
