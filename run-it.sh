#!/bin/bash

rm -rf /tmp/spark-events/*
rm -rf /tmp/spark-logs/*

#echo "Building Spark..."
#build/sbt package >& /dev/null

for strategy in "avg" "ewma"; do
  for window_size in 1 10 50; do
    log_file_path=/tmp/spark-logs/mlpc_"$window_size""$strategy".log
    echo "Running strategy: "$strategy", window size: "$window_size", logging to $log_file_path"
    bin/spark-submit --master local-cluster[4,8,1000000] --driver-memory 2g --conf spark.executor.memory=15g --conf spark.scheduler.mode=FAIR --conf spark.approximation.predLoss.windowSize="$window_size" --conf spark.approximation.predLoss.strategy="$strategy" --class org.apache.spark.examples.ml.MultilayerPerceptronClassifierExample examples/target/scala-2.11/jars/spark-examples_2.11-2.1.0.jar >& "$log_file_path"
  done
done

