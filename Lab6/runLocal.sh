#!/bin/sh
set -eu
cd -- "$(dirname -- "$0")"
if [ "$#" -eq 0 ]; then
    set -- data/ReviewsSample.csv output
fi

spark-submit --class it.polito.bigdata.spark.example.SparkDriver --master "local[*]" --deploy-mode client target/Lab6-1.0.0.jar "$@"
