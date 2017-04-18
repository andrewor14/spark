#!/bin/bash

rm -rf /tmp/spark-events/*
rm -rf /tmp/spark-logs/*

conf_prefix="spark.approximation.predLoss"

function run_spark() {
  extra_conf="$1"
  log_file_path="$2"
  bin/spark-submit\
    --master local-cluster[1,16,1000000]\
    --driver-memory 3g\
    --conf spark.executor.memory=50g\
    --conf spark.scheduler.mode=FAIR\
    $extra_conf\
    --class org.apache.spark.examples.ml.MultilayerPerceptronClassifierExample\
    examples/target/scala-2.11/jars/spark-examples_2.11-2.1.0.jar >& "$log_file_path"
}

## AVG
#for window_size in 1 10; do
#  extra_conf="--conf $conf_prefix.strategy=avg --conf $conf_prefix.windowSize=$window_size"
#  log_file_path=/tmp/spark-logs/mlpc_avg_"$window_size".log
#  echo "Running avg with window size $window_size, logging to $log_file_path"
#  run_spark "$extra_conf" "$log_file_path"
#done
#
## EWMA
#for alpha in 0.8; do
#  extra_conf="--conf $conf_prefix.strategy=ewma --conf $conf_prefix.ewma.alpha=$alpha"
#  log_file_path=/tmp/spark-logs/mlpc_ewma_"$alpha".log
#  echo "Running ewma with alpha $alpha, logging to $log_file_path"
#  run_spark "$extra_conf" "$log_file_path"
#done

# CF
for decay in 0.6 0.7 0.8 0.9; do
  extra_conf="--conf $conf_prefix.strategy=cf --conf $conf_prefix.cf.decay=$decay"
  log_file_path=/tmp/spark-logs/mlpc_cf_"$decay".log
  echo "Running cf with decay $decay, logging to $log_file_path"
  run_spark "$extra_conf" "$log_file_path"
done

