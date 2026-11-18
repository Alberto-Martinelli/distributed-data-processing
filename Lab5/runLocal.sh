#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- SampleLocalFile.csv output ho
fi

spark-submit --class it.polito.bigdata.spark.example.SparkDriver --master "local[*]" --deploy-mode client target/Lab5-1.0.0.jar "$@"
