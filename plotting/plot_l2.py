#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

from os.path import join
from plot import *

iterations_of_interest = [1, 5, 10] # Must be length 3
curve_to_fit = "one_over_x_squared"
decay_to_fit = 0.6

def get_data(path):
  l2 = []
  for predicted_n_iterations_ago in iterations_of_interest:
    (actual_x, actual_y, predicted_x, predicted_y) =\
      parse_losses(path, predicted_n_iterations_ago)
    l2 += [calculate_l2_norm(actual_x, actual_y, predicted_x, predicted_y)]
  print "L2 norms for %s for these iterations %s are %s" % (path.split("/")[-1], iterations_of_interest, l2)
  return l2

def get_normalized_data(path1, path2, path3):
  data1 = get_data(path1)
  data2 = get_data(path2)
  data3 = get_data(path3)
  # Normalize everything by the worst error
  worst_error = max(data1 + data2 + data3)
  for i in range(len(data1)):
    data1[i] = data1[i] / worst_error
    data2[i] = data2[i] / worst_error
    data3[i] = data3[i] / worst_error
  return (data1, data2, data3)

def main():
  args = sys.argv
  if len(args) < 2:
    print "Expected at least one log directory."
    sys.exit(1)
  out_dir = args[1] if len(args) == 2 else "."
  log_dirs = args[1:]

  # Some random variables
  algorithm_names = [d.split("/")[-1] for d in log_dirs]
  num_algorithms = len(algorithm_names)
  num_bars_per_group = 2 + 3 * num_algorithms
  width = 1.0 / num_bars_per_group
  indices = np.arange(3)
  log_file_names = ["avg_1.log", "cf_%s_1.log" % curve_to_fit, "cf_%s_%s.log" % (curve_to_fit, decay_to_fit)]
  empty = [0, 0, 0]
  color_cycle = ["r", "m", "b", "c", "y", "g", "k", "w"]

  # Start plotting
  fig, ax = plt.subplots(figsize=(20, 8))
  log_file_paths = []
  bars = []
  bars += [ax.bar(indices, empty, width)]
  for i in range(len(log_dirs)):
    naive_path = join(log_dirs[i], log_file_names[0])
    curve_fitting_path = join(log_dirs[i], log_file_names[1])
    curve_fitting_weighted_path = join(log_dirs[i], log_file_names[2])
    (naive, curve_fitting, curve_fitting_weighted) =\
      get_normalized_data(naive_path, curve_fitting_path, curve_fitting_weighted_path)
    base_indices = indices + 3 * width * i
    fill_color = color_cycle[i]
    edge_color = "w" if fill_color == "k" else "k"
    bars += [ax.bar(base_indices + width, naive, width, color=fill_color, edgecolor=edge_color, hatch="/")]
    bars += [ax.bar(base_indices + 2 * width, curve_fitting, width, color=fill_color, edgecolor=edge_color, hatch="-")]
    bars += [ax.bar(base_indices + 3 * width, curve_fitting_weighted, width, color=fill_color, edgecolor=edge_color, hatch="\\")]
  bars += [ax.bar(indices + (3 * num_algorithms + 1) * width, empty, width)]
  ax.set_ylabel("Normalized L2 norm of prediction error")
  ax.set_xlabel("Number of iterations predicted in advance")
  ax.set_title("Loss prediction error", y = 1.04)
  ax.set_xticks(indices + width * num_bars_per_group / 2)
  ax.set_xticklabels(("1", "5", "10"))
  plt.gca().set_ylim([0.0, 1.04])
  legend_bars = tuple([r[0] for r in bars[1:-1]])
  legend_names = []
  for alg_name in algorithm_names:
    for i in range(len(log_file_names)):
      legend_names += ["%s (%s)" % (translate_legend(log_file_names[i]), alg_name)]
  legend_names = tuple(legend_names)
  box = ax.get_position()
  ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
  ax.legend(legend_bars, legend_names, prop = {"size": 12}, loc = "center left", bbox_to_anchor=(1.04, 0.5))
  plt.savefig(join(out_dir, "prediction_error_%s.png" % translate_name(log_file_names[-1])))

if __name__ == "__main__":
  main()

