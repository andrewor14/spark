#!/bin/bash

base_dir=$1
loss_file="$1/losses.txt"

./run-sim.sh "$base_dir" avg 1
./run-sim.sh "$base_dir" cf 1 OneOverXSquaredFunctionFitter 10
./run-sim.sh "$base_dir" cf 0.5 OneOverXSquaredFunctionFitter 100
./plotting/plot.py "$base_dir" \
  "sim_mlpc_avg_1.log" \
  "sim_mlpc_cf_OneOverXSquaredFunctionFitter_1.log" \
  "sim_mlpc_cf_OneOverXSquaredFunctionFitter_0.5.log"
./plotting/plot_l2.py "$base_dir"

