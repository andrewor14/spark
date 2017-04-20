#!/bin/bash

base_dir=$1
loss_file="$1/losses.txt"

func="OneOverXSquaredFunctionFitter"

./run-sim.sh "$base_dir" avg 1
./run-sim.sh "$base_dir" cf 1 $func 10
./run-sim.sh "$base_dir" cf 0.5 $func 100
./plotting/plot.py "$base_dir" \
  "sim_mlpc_avg_1.log" \
  "sim_mlpc_cf_"$func"_1.log" \
  "sim_mlpc_cf_"$func"_0.5.log"
./plotting/plot_l2.py "$base_dir"
  "sim_mlpc_avg_1.log" \
  "sim_mlpc_cf_"$func"_1.log" \
  "sim_mlpc_cf_"$func"_0.5.log"

