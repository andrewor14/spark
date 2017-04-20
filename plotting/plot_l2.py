#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

from plot import *

iterations_of_interest = [1, 5, 10]

def get_data(path):
  l2 = []
  for predicted_n_iterations_ago in iterations_of_interest:
    (actual_x, actual_y, predicted_x, predicted_y) =\
      parse_losses(path, predicted_n_iterations_ago)
    l2 += [calculate_l2_norm(actual_x, actual_y, predicted_x, predicted_y)]
  return l2

def main():
  indices = np.arange(3)
  width = 0.2
  fig, ax = plt.subplots()
  empty = [0, 0, 0]
  naive = get_data("sim_mlpc_avg_1.log")
  one_over_x_squared = get_data("sim_mlpc_cf_OneOverXSquaredFunctionFitter_1.log")
  one_over_x_squared_weighted = get_data("sim_mlpc_cf_OneOverXSquaredFunctionFitter_0.5.log")
  rects0 = ax.bar(indices, empty, width)
  rects1 = ax.bar(indices + width, naive, width, color="r")
  rects2 = ax.bar(indices + 2 * width, one_over_x_squared, width, color="m")
  rects3 = ax.bar(indices + 3 * width, one_over_x_squared_weighted, width, color="b")
  ax.set_ylabel("Prediction error L2 norm")
  ax.set_xlabel("Number of iterations predicted in advance")
  ax.set_title("MLPC loss prediction error", y = 1.04)
  ax.set_xticks(indices + width * 2.5)
  ax.set_xticklabels(("1", "5", "10"))
  ax.legend(\
    (rects1[0], rects2[0], rects3[0]),\
    ("naive", "1 / x^2", "1 / x^2 weighted"),\
    prop = {"size": 12},\
    loc = "upper left")
  plt.savefig("prediction_error.png")

if __name__ == "__main__":
  main()

