#!/bin/bash

base_dir="$1"
loss_file="$1/losses.txt"

do_the_thing() {
  func="$1"
  ./run-sim.sh "$base_dir" avg 1
  ./run-sim.sh "$base_dir" cf 1 "$func" 10
  for decay in 0.6 0.7 0.8 0.9; do
    ./run-sim.sh "$base_dir" cf "$decay" "$func" 100
  done
  ./plotting/plot.py "$base_dir" \
    "avg_1.log" \
    "cf_"$func"_1.log" \
    "cf_"$func"_0.8.log"
  ./plotting/plot_l2.py "$base_dir"
}

do_the_thing "one_over_x"
do_the_thing "one_over_x_squared"

