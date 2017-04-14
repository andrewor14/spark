#!/usr/bin/env python

import matplotlib.pyplot as plt
import math
import re
import sys

args = sys.argv
verbose = True

if len(args) != 2:
  print "Expected log file."
  sys.exit(1)

log_file_name = args[1]

# Parse actual and predicted losses from log
actual_loss_data = {} # (iteration, cores) -> loss
predicted_loss_data = {} # (iteration, cores) -> loss
with open(log_file_name) as f:
  cool_lines = [line for line in f.readlines() if "ANDREW" in line and "loss = " in line]
  for line in cool_lines:
    iteration = int(re.match(".*ANDREW\((.*)\):.*", line).groups()[0])
    cores = float(re.match(".*\(cores = (\d*)\).*", line).groups()[0])
    loss = float(re.match(".*\(.* loss = (.*)\).*", line).groups()[0])
    if "actual" in line:
      actual_loss_data[(iteration, cores)] = loss
    elif "predicted" in line:
      predicted_loss_data[(iteration, cores)] = loss
    else:
      print "Yikes, bad line:\n\t%s" % line

# Only keep the predicted losses whose number of cores correspond to that of the actual losses
actual_x = []
actual_y = []
predicted_x = []
predicted_y = []
for (iteration, cores), loss in actual_loss_data.items():
  actual_x += [iteration]
  actual_y += [loss]
  if (iteration, cores) in predicted_loss_data.keys():
    predicted_x += [iteration]
    predicted_y += [predicted_loss_data[(iteration, cores)]]
  elif verbose:
    print "Yikes, no corresponding predicted loss for iteration %s and %s cores" %\
      (iteration, cores)

# What's the difference between the actual loss and the predicted loss?
l2_differences = []
for i, pred_loss in enumerate(predicted_y):
  time = predicted_x[i]
  if time in actual_x:
    j = actual_x.index(time)
    act_loss = actual_y[j]
    l2_differences += [math.pow(pred_loss - act_loss, 2)]
print "L2 norm: %s" % sum(l2_differences)

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
ax.set_title(log_file_name)
plt.legend()
plt.savefig(log_file_name.replace("log", "png"))

