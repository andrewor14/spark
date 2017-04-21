#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

from os.path import join
from plot import *

iterations_of_interest = [1, 5, 10]

def get_data(path):
  l2 = []
  for predicted_n_iterations_ago in iterations_of_interest:
    (actual_x, actual_y, predicted_x, predicted_y) =\
      parse_losses(path, predicted_n_iterations_ago)
    l2 += [calculate_l2_norm(actual_x, actual_y, predicted_x, predicted_y)]
  print "L2 norms for %s for these iterations %s are %s" % (path.split("/")[-1], iterations_of_interest, l2)
  return l2

def main():
  args = sys.argv
  if len(args) != 5:
    print "Expected base dir."
    sys.exit(1)
  base_dir = args[1]
  naive_path = join(base_dir, args[2])
  curve_fitting_path = join(base_dir, args[3])
  curve_fitting_weighted_path = join(base_dir, args[4])
  indices = np.arange(3)
  width = 0.2
  fig, ax = plt.subplots()
  empty = [0, 0, 0]
  naive = get_data(naive_path)
  curve_fitting = get_data(curve_fitting_path)
  curve_fitting_weighted = get_data(curve_fitting_weighted_path)
  rects0 = ax.bar(indices, empty, width)
  rects1 = ax.bar(indices + width, naive, width, color="r")
  rects2 = ax.bar(indices + 2 * width, curve_fitting, width, color="m")
  rects3 = ax.bar(indices + 3 * width, curve_fitting_weighted, width, color="b")
  ax.set_ylabel("Prediction error L2 norm")
  ax.set_xlabel("Number of iterations predicted in advance")
  ax.set_title("%s loss prediction error" % base_dir.split("/")[-1], y = 1.04)
  ax.set_xticks(indices + width * 2.5)
  ax.set_xticklabels(("1", "5", "10"))
  ax.legend(\
    (rects1[0], rects2[0], rects3[0]),\
    ("naive", translate_legend(curve_fitting_path), translate_legend(curve_fitting_weighted_path)),\
    prop = {"size": 12},\
    loc = "upper left")
  plt.savefig(join(base_dir, "prediction_error_%s.png" % translate_name(curve_fitting_path)))

if __name__ == "__main__":
  main()

