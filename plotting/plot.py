#!/usr/bin/env python

import matplotlib.pyplot as plt
import math
import numpy as np
import os
import re
import sys


verbose = False
min_iteration_for_prediction = 2
predicted_n_iterations_ago = 10

def main():
  args = sys.argv
  # User supplied exactly 1 log file / directory
  if len(args) == 2:
    log_file_path = args[1]
    if os.path.isfile(log_file_path):
      do_the_thing(".", log_file_path)
    elif os.path.isdir(log_file_path):
      base_dir = log_file_path
      for f in os.listdir(base_dir):
        if f.endswith(".log"):
          do_the_thing(base_dir, f)
  # User supplied more than one log file. Plot them all on the same graph.
  # Note that first argument is the base directory.
  elif len(args) > 2:
    do_the_thing(args[1], *args[2:])
  else:
    print "Expected log file."
    sys.exit(1)

def do_the_thing(base_dir, *log_file_paths):
  assert len(log_file_paths) > 0
  log_file_paths = [os.path.join(base_dir, f) for f in log_file_paths]
  actual_x = []
  actual_y = []
  out_file_path = None
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Loss")
  ax.set_color_cycle(['b', 'g', 'c', 'r'])

  # Plot the actual and predicted losses
  for path in log_file_paths:
    (my_actual_x, my_actual_y, predicted_x, predicted_y) =\
      parse_losses(path, predicted_n_iterations_ago)
    if not actual_x and not actual_y:
      actual_x = my_actual_x
      actual_y = my_actual_y
      ax.plot(actual_x, actual_y, label="actual", linewidth=3.0)
    else:
      assert actual_x == my_actual_x
      assert actual_y == my_actual_y,\
        "first actual_y = %s,\nmy_actual_y = %s" % (actual_y, my_actual_y)
    l2_norm = calculate_l2_norm(actual_x, actual_y, predicted_x, predicted_y)
    name = translate_legend(path)
    ax.plot(predicted_x, predicted_y, label="%s, L2 norm = %.3f" % (name, l2_norm))
    print "L2 norm for %s: %s" % (path, l2_norm)

  plt.legend(prop={'size':12})
  if len(log_file_paths) == 1:
    out_file_path = os.path.join(base_dir, log_file_paths[0].replace("log", "png"))
    ax.set_title(log_file_paths[0], y = 1.04)
  else:
    out_file_path = os.path.join(base_dir, "prediction_%s.png" % translate_name(log_file_paths[-1]))
    experiment_name = base_dir.split("/")[-1]
    ax.set_title("%s loss prediction (%s iterations in advance)" %\
      (experiment_name, predicted_n_iterations_ago), y = 1.04)
  plt.savefig(out_file_path)

def translate_name(name):
  if "avg_1" in name:
    return "naive"
  elif "cf" in name:
    if "exponential" in name: return "exponential"
    if "one_over_x_squared" in name: return "one_over_x_squared"
    if "one_over_x" in name: return "one_over_x"
  else:
    return name

def translate_legend(name):
  if "cf" in name:
    func = "curve fitting"
    if "one_over_x" in name: func = "1 / x"
    if "one_over_x_squared" in name: func = "1 / x^2"
    if "exponential" in name: func = "exp(-x)"
    decay = float(re.match(".*cf_.*_(.*).log", name).groups()[0])
    maybe_weighted = " weighted" if decay < 1 else ""
    return func + maybe_weighted
  else:
    return translate_name(name)

def parse_losses(log_file_path, predicted_n_iterations_ago):
  # Parse actual and predicted losses from log
  actual_loss_data = {} # iteration -> loss
  predicted_loss_data = {} # iteration -> list of losses, ordered by time first seen
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
        if iteration >= min_iteration_for_prediction:
          if iteration not in predicted_loss_data:
            predicted_loss_data[iteration] = []
          predicted_loss_data[iteration] += [loss]
      else:
        print "Yikes, bad line:\n\t%s" % line

  # Only keep the predicted losses whose iteration number that corresponds
  # to those of the actual losses
  actual_x = []
  actual_y = []
  predicted_x = []
  predicted_y = []
  for iteration, loss in actual_loss_data.items():
    actual_x += [iteration]
    actual_y += [loss]
    if iteration in predicted_loss_data.keys():
      predicted_x += [iteration]
      predicted_losses = list(reversed(predicted_loss_data[iteration]))
      index = min(predicted_n_iterations_ago - 1, len(predicted_losses) - 1)
      predicted_y += [predicted_losses[index]]
    elif verbose:
      print "Yikes, no corresponding predicted loss for iteration %s" % iteration

  # Sort the points
  actual_points = [(actual_x[i], actual_y[i]) for i in range(len(actual_x))]
  predicted_points = [(predicted_x[i], predicted_y[i]) for i in range(len(predicted_x))]
  actual_points.sort(key = lambda x: x[0])
  predicted_points.sort(key = lambda x: x[0])
  actual_x = [x for (x, _) in actual_points]
  actual_y = [y for (_, y) in actual_points]
  predicted_x = [x for (x, _) in predicted_points]
  predicted_y = [y for (_, y) in predicted_points]
  return (actual_x, actual_y, predicted_x, predicted_y)

def calculate_l2_norm(actual_x, actual_y, predicted_x, predicted_y):
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
  return sum(l2_differences)
 
if __name__ == "__main__":
  main()

