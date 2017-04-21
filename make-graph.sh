#!/bin/bash

base_dir=$1
loss_file="$1/losses.txt"

decay=0.8

do_the_thing() {
  func="$1"
  ./run-sim.sh "$base_dir" avg 1
  ./run-sim.sh "$base_dir" cf 1 $func 50
  ./run-sim.sh "$base_dir" cf $decay $func 100
  ./plotting/plot.py "$base_dir" \
    "avg_1.log" \
    "cf_"$func"_1.log" \
    "cf_"$func"_$decay.log"
  ./plotting/plot_l2.py "$base_dir"
}

do_the_thing "one_over_x"
do_the_thing "one_over_x_squared"

