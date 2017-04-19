#!/bin/bash

strategy="$1"
decay="$2"
fitterName="$3"

log_file_name=sim_mlpc_"$strategy"_"$fitterName"_"$decay"

bin/spark-submit --conf spark.approximation.predLoss.strategy="$strategy" --conf spark.approximation.predLoss.cf.decay="$decay" --conf spark.approximation.predLoss.cf.fitterName="$fitterName" --class org.apache.spark.BestCurveFitterEver my.jar plotting/actual_losses.txt plotting/"$log_file_name".log &> plotting/"$log_file_name".loglog

