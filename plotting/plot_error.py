#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

from os.path import join
from plot import *

iterations_of_interest = [1, 5, 10] # Must be length 3
decay_to_fit = 0.8
alg_to_curve_fitter = {
  "gbt": "one_over_x_squared",
  "lda": "one_over_x_squared",
  "svm": "one_over_x_squared",
  "mlpc": "exponential",
  "logreg": "exponential",
  "linreg": "exponential"
}

def get_data(path):
  avg_error = []
  for predicted_n_iterations_ago in iterations_of_interest:
    (actual_x, actual_y, predicted_x, predicted_y) =\
      parse_losses(path, predicted_n_iterations_ago)
    avg_error += [calculate_avg_abs_error(actual_x, actual_y, predicted_x, predicted_y)]
  print "Average prediction error for %s for these iterations %s are %s" % (path, iterations_of_interest, avg_error)
  return avg_error

def main():
  args = sys.argv
  if len(args) < 2:
    print "Expected at least one log directory."
    sys.exit(1)
  out_dir = args[1] if len(args) == 2 else "."
  log_dirs = args[1:]

  # Some random variables
  num_algrithms = len(log_dirs)
  num_bars_per_group = 2 + 3 * num_algrithms
  width = 1.0 / num_bars_per_group
  indices = np.arange(3)
  empty = [0, 0, 0]
  color_cycle = ["skyblue", "lightgrey", "red", "yellowgreen", "gold", "pink", "lightseagreen", "coral", "blueviolet"]

  # Start plotting
  fig, ax = plt.subplots(figsize=(20, 8))
  log_file_paths = []
  bars = []
  bars += [ax.bar(indices, empty, width)]
  legend_names = []
  for i in range(len(log_dirs)):
    alg_name = log_dirs[i].split("/")[-1]
    curve_to_fit = None
    for alg in alg_to_curve_fitter:
      if alg in alg_name:
        curve_to_fit = alg_to_curve_fitter[alg]
    log_file_names = ["avg_1.log", "cf_%s_1.log" % curve_to_fit, "cf_%s_%s.log" % (curve_to_fit, decay_to_fit)]
    naive_path = join(log_dirs[i], log_file_names[0])
    curve_fitting_path = join(log_dirs[i], log_file_names[1])
    curve_fitting_weighted_path = join(log_dirs[i], log_file_names[2])
    naive = get_data(naive_path)
    curve_fitting = get_data(curve_fitting_path)
    curve_fitting_weighted = get_data(curve_fitting_weighted_path)
    base_indices = indices + 3 * width * i
    fill_color = color_cycle[i]
    edge_color = "w" if fill_color == "k" else "k"
    bars += [ax.bar(base_indices + width, naive, width, color=fill_color, edgecolor=edge_color, hatch="/", bottom=0.001)]
    bars += [ax.bar(base_indices + 2 * width, curve_fitting, width, color=fill_color, edgecolor=edge_color, hatch="-", bottom=0.001)]
    bars += [ax.bar(base_indices + 3 * width, curve_fitting_weighted, width, color=fill_color, edgecolor=edge_color, hatch="\\", bottom=0.001)]
    for i in range(len(log_file_names)):
      legend_names += ["%s (%s)" % (translate_legend(log_file_names[i]), alg_name)]
  bars += [ax.bar(indices + (3 * num_algrithms + 1) * width, empty, width)]
  ax.set_ylabel("Average prediction error")
  ax.set_xlabel("Number of iterations predicted in advance")
  ax.set_title("Loss prediction error", y = 1.04)
  ax.set_xticks(indices + width * num_bars_per_group / 2)
  ax.set_xticklabels(("1", "5", "10"))
  #ax.set_yscale("log")
  legend_bars = tuple([r[0] for r in bars[1:-1]])
  legend_names = tuple(legend_names)
  box = ax.get_position()
  ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
  ax.legend(legend_bars, legend_names, prop = {"size": 12}, loc = "center left", bbox_to_anchor=(1.04, 0.5))
  plt.savefig(join(out_dir, "prediction_error.png"))

if __name__ == "__main__":
  main()

