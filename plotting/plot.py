#!/usr/bin/env python

import matplotlib.pyplot as plt
import math
import numpy as np
import os
import re
import sys


verbose = False

def main():
  args = sys.argv
  if len(args) != 2:
    print "Expected log file."
    sys.exit(1)
  log_file_path = args[1]
  if os.path.isfile(log_file_path):
    do_the_thing(log_file_path)
  elif os.path.isdir(log_file_path):
    for f in os.listdir(log_file_path):
      if f.endswith(".log"):
        do_the_thing(f)

def do_the_thing(log_file_path):
  # Parse actual and predicted losses from log
  actual_loss_data = {} # iteration -> loss
  predicted_loss_data = {} # iteration -> loss
  with open(log_file_path) as f:
    cool_lines = [line for line in f.readlines() if "ANDREW" in line and "loss = " in line]
    for line in cool_lines:
      iteration = int(re.match(".*ANDREW\((.*)\):.*", line).groups()[0])
      loss = float(re.match(".*\(.* loss = (.*)\).*", line).groups()[0])
      if "actual" in line:
        # Take the first prediction for this iteration
        if iteration not in actual_loss_data:
          actual_loss_data[iteration] = loss
      elif "predicted" in line:
        # Take the first prediction for this iteration
        if iteration not in predicted_loss_data and iteration > 10:
          predicted_loss_data[iteration] = loss
      else:
        print "Yikes, bad line:\n\t%s" % line
  
  # Only keep the predicted losses iteration number corresponds to that of the actual losses
  actual_x = []
  actual_y = []
  predicted_x = []
  predicted_y = []
  for iteration, loss in actual_loss_data.items():
    actual_x += [iteration]
    actual_y += [loss]
    if iteration in predicted_loss_data.keys():
      predicted_x += [iteration]
      predicted_y += [predicted_loss_data[iteration]]
    elif verbose:
      print "Yikes, no corresponding predicted loss for iteration %s" % iteration
  
  # What's the difference between the actual loss and the predicted loss?
  l2_differences = []
  percentile_cutoff = 99
  for i, pred_loss in enumerate(predicted_y):
    time = predicted_x[i]
    if time in actual_x:
      j = actual_x.index(time)
      act_loss = actual_y[j]
      l2_differences += [math.pow(pred_loss - act_loss, 2)]
  cutoff = np.percentile(l2_differences, percentile_cutoff)
  l2_differences = [d for d in l2_differences if d <= cutoff]
  l2_norm = sum(l2_differences)
  print "L2 norm for %s: %s" % (log_file_path, l2_norm)
  
  # Sort the points
  actual_points = [(actual_x[i], actual_y[i]) for i in range(len(actual_x))]
  predicted_points = [(predicted_x[i], predicted_y[i]) for i in range(len(predicted_x))]
  actual_points.sort(key = lambda x: x[0])
  predicted_points.sort(key = lambda x: x[0])
  actual_x = [x for (x, _) in actual_points]
  actual_y = [y for (_, y) in actual_points]
  predicted_x = [x for (x, _) in predicted_points]
  predicted_y = [y for (_, y) in predicted_points]
  
  # Plot it!
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(actual_x, actual_y, label="actual")
  ax.plot(predicted_x, predicted_y, label="predicted")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Loss")
  ax.set_title(log_file_path)
  ax.text(10, 0.8, "L2 norm: %s" % l2_norm)
  plt.legend()
  plt.savefig(log_file_path.replace("log", "png"))

if __name__ == "__main__":
  main()

