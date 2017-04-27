#!/bin/bash

base_dir="$1"
strategy="$2"
param1="$3"
param2="$4"
param3="$5"

if [ "$strategy" == "cf" ]; then
  decay="$param1"
  fitterName="$param2"
  windowSize="$param3"
  extra_conf="--conf spark.approximation.predLoss.cf.decay=$decay"
  extra_conf="$extra_conf --conf spark.approximation.predLoss.cf.fitterName=$fitterName"
  extra_conf="$extra_conf --conf spark.approximation.predLoss.windowSize=$windowSize"
  log_file_name="$strategy"_"$fitterName"_"$decay"
elif [ "$strategy" == "avg" ]; then
  windowSize="$param1"
  extra_conf="--conf spark.approximation.predLoss.windowSize=$windowSize"
  log_file_name="$strategy"_"$windowSize"
elif [ "$strategy" == "ewma" ]; then
  alpha="$param1"
  extra_conf="--conf spark.approximation.predLoss.ewma.alpha=$alpha"
  log_file_name="$strategy"_"$alpha"
fi

conf="--conf spark.approximation.predLoss.strategy=$strategy"
conf="$conf --conf spark.approximation.predLoss.numIterations=10"
conf="$conf $extra_conf"
echo "Running simulation with conf: $conf, writing to log file: $base_dir/$log_file_name.log"

plotting/best_curve_fitter_ever.py "$base_dir/losses.txt" "$base_dir/$log_file_name".log $conf &> "$base_dir/$log_file_name".loglog

