#!/usr/bin/env python

import sys

import math
import matplotlib.pyplot as plt
import numpy as np
import scipy
from scipy.optimize import curve_fit

def main():
  args = sys.argv
  if len(args) < 5:
    print "Expected curve_type, x, y, and decay, (and maybe starting parameters)"
    sys.exit(1)
  curve_type = args[1]
  x = [float(t) for t in args[2].split(",")]
  y = [float(t) for t in args[3].split(",")]
  decay = float(args[4])
  if len(args) == 5:
    fit_curve(curve_type, x, y, decay)
  elif len(args) == 6:
    starting_params = [float(t) for t in args[5].split(",")]
    fit_curve(curve_type, x, y, decay, starting_params)
  else:
    print "Too many params dude: %s" % args
    sys.exit(1)

def fit_curve(curve_type, x, y, decay=0.9, starting_params=None):
  func = get_func(curve_type)
  num_parameters = get_num_parameters(curve_type)
  sigma = [math.pow(decay, i) for i in range(len(y))]
  bounds = ([0] * num_parameters, [np.inf] * num_parameters)
  coeffs = None
  if starting_params is not None:
    coeffs = curve_fit(func, x, y, p0=starting_params, sigma=sigma, bounds=bounds)[0]
  else:
    coeffs = curve_fit(func, x, y, sigma=sigma, bounds=bounds)[0]
  print ", ".join([str(c) for c in coeffs])

def one_over_x(x, a, b, c):
  return 1 / (a * x + b) + c

def one_over_x_squared(x, a, b, c, d):
  return 1 / (a * (x**2) + b * x + c) + d

def get_func(curve_type):
  if curve_type == "one_over_x": return one_over_x
  if curve_type == "one_over_x_squared": return one_over_x_squared
  return None

def get_num_parameters(curve_type):
  if curve_type == "one_over_x": return 3
  if curve_type == "one_over_x_squared": return 4
  return None

if __name__ == "__main__":
  main()

