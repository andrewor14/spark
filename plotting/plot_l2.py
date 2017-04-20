#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np
import sys

naive = [0.0104850561518, 0.230559622847, 0.867949986696]
one_over_x_squared_weighted = [2.30675169237, 0.169151590578, 0.57741398472]
one_over_x_squared = [0.0260023147945, 2.73528843391, 3.29251997264]

def main():
  indices = np.arange(3)
  width = 0.2
  fig, ax = plt.subplots()
  empty = (0, 0, 0)
  rects0 = ax.bar(indices, empty, width)
  rects1 = ax.bar(indices + width, naive, width, color="r")
  rects2 = ax.bar(indices + 2 * width, one_over_x_squared, width, color="m")
  rects3 = ax.bar(indices + 3 * width, one_over_x_squared_weighted, width, color="b")
  ax.set_ylabel("Prediction error L2 norm")
  ax.set_xlabel("Number of iterations predicted in advance")
  axes = plt.gca()
  axes.set_ylim([0, 3.7])
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

