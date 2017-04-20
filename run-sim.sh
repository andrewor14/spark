#!/bin/bash

strategy="$1"
param1="$2"
param2="$3"
param3="$4"

if [ "$strategy" == "cf" ]; then
  decay="$param1"
  fitterName="$param2"
  windowSize="$param3"
  extra_conf="--conf spark.approximation.predLoss.cf.decay=$decay"
  extra_conf="$extra_conf --conf spark.approximation.predLoss.cf.fitterName=$fitterName"
  extra_conf="$extra_conf --conf spark.approximation.predLoss.windowSize=$windowSize"
  log_file_name=sim_mlpc_"$strategy"_"$fitterName"_"$decay"
elif [ "$strategy" == "avg" ]; then
  windowSize="$param1"
  extra_conf="--conf spark.approximation.predLoss.windowSize=$windowSize"
  log_file_name=sim_mlpc_"$strategy"_"$windowSize"
elif [ "$strategy" == "ewma" ]; then
  alpha="$param1"
  extra_conf="--conf spark.approximation.predLoss.ewma.alpha=$alpha"
  log_file_name=sim_mlpc_"$strategy"_"$alpha"
fi

echo "$extra_conf"

bin/spark-submit --conf spark.approximation.predLoss.strategy="$strategy" --conf spark.approximation.predLoss.numIterations=10 $extra_conf --class org.apache.spark.BestCurveFitterEver my.jar plotting/actual_losses.txt plotting/"$log_file_name".log &> plotting/"$log_file_name".loglog

