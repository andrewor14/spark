#!/usr/bin/env python

import math
import matplotlib.pyplot as plt
import re
import sys


def main():
  args = sys.argv
  if len(args) < 3 or len(args) > 4:
    print "Usage: test_plot.py <log_file_name> <prediction iteration> [<zoom iteration>]"
    sys.exit(1)
  log_file_name = args[1]
  prediction_iter = int(args[2])
  zoom_iter = None
  if len(args) == 4:
    zoom_iter = int(args[3])
    assert zoom_iter < prediction_iter
  # Parse points and curve coefficients
  x, y, params = [], [], []
  with open(log_file_name + "log") as f:
    lines = f.readlines()
    for i in range(len(lines)):
      line = lines[i]
      if re.match(".*ANDREW predicting in iteration %s$" % prediction_iter, line) is not None:
        x = [float(t) for t in re.match(".*My x's are: (.*)\.", lines[i+1]).groups()[0].split(",")]
        y = [float(t) for t in re.match(".*My y's are: (.*)\.", lines[i+2]).groups()[0].split(",")]
        params = [float(t) for t in re.match(".*My params are (.*)", lines[i+3]).groups()[0].split(",")]
        break
  # Parse future points
  num_extra_points = 10
  extra_x_start = int(x[-1]) + 1
  extra_x = range(extra_x_start, extra_x_start + num_extra_points)
  extra_y = []
  with open(log_file_name) as f:
    for line in f.readlines():
      match = re.match("ANDREW\(([0-9]*)\): \(actual loss = ([0-9.]*)\)", line)
      if match is not None:
        index = int(match.groups()[0]) - 1
        loss = float(match.groups()[1])
        if index in extra_x:
          extra_y += [loss]
  print "x = %s" % x
  print "y = %s" % y
  print "extra_x = %s" % extra_x
  print "extra_y = %s" % extra_y
  # Maybe zoom in a little
  if zoom_iter is not None and zoom_iter > x[0]:
    index = x.index(float(zoom_iter))
    x = x[index:]
    y = y[index:]
  # Plot
  if "one_over_x_squared" in log_file_name:
    plot_one_over_x_squared(x, y, extra_x, extra_y, params)
  elif "one_over_x" in log_file_name:
    plot_one_over_x(x, y, extra_x, extra_y, params)
  else:
    print "Error: Unknown curve type %s" % log_file_name
    sys.exit(1)

def plot(call_func, x, y, extra_x, extra_y, nice_string, *params):
  fitted_y = [call_func(xx) for xx in x + extra_x]
  l2_error = l2_diff(extra_y, fitted_y[-len(extra_y):])
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(x, y, "x", label="orig")
  ax.plot(x + extra_x, fitted_y, label="fitted")
  ax.plot(extra_x, extra_y, "x", label="future")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Loss")
  plt.annotate(nice_string, xy=(0.6, 0.8), xycoords='axes fraction')
  plt.annotate("L2 error = %.10f" % l2_error, xy=(0.6, 0.7), xycoords='axes fraction', color='red')
  plt.savefig("output.png")

def plot_one_over_x_squared(x, y, extra_x, extra_y, params):
  assert len(params) == 4
  (a, b, c, d) = tuple(params)
  call_func = lambda x: 1 / (a * math.pow(x, 2) + b * x + c) + d
  nice_string = "1 / (ax^2 + bx + c) + d\na = %s\nb = %s\nc = %s\nd = %s" % (a, b, c, d)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b, c, d)

def plot_one_over_x(x, y, extra_x, extra_y, params):
  assert len(params) == 3
  (a, b) = tuple(params)
  call_func = lambda x: 1 / (a * x + b) + c
  nice_string = "1 / (ax + b) + c\na = %s\nb = %s\nc = %s" % (a, b, c)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b, c)

def l2_diff(list1, list2):
  assert len(list1) == len(list2)
  return sum([math.pow(list1[i] - list2[i], 2) for i in range(len(list1))])

if __name__ == "__main__":
  main()

