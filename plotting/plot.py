#!/usr/bin/env python

import matplotlib.pyplot as plt
import math
import re
import sys

args = sys.argv

if len(args) != 2:
  print "Expected log file."
  sys.exit(1)

log_file_name = args[1]

# Parse actual and predicted losses from log
actual_x = []
actual_loss = []
predicted_x = []
predicted_loss = []
with open(log_file_name) as f:
  cool_lines = [line for line in f.readlines() if "ANDREW" in line and "loss = " in line]
  for line in cool_lines:
    iteration = int(re.match(".*ANDREW\((.*)\):.*", line).groups()[0])
    loss = float(re.match(".*\(.* loss = (.*)\).*", line).groups()[0])
    if "actual" in line:
      actual_x += [iteration]
      actual_loss += [loss]
    elif "predicted" in line:
      predicted_x += [iteration]
      predicted_loss += [loss]
    else:
      print "Yikes, bad line:\n\t%s" % line

# What's the difference between the actual loss and the predicted loss?
differences = []
for i, pred_loss in enumerate(predicted_loss):
  time = predicted_x[i]
  if time in actual_x:
    j = actual_x.index(time)
    act_loss = actual_loss[j]
    differences += [math.pow(pred_loss - act_loss, 2)]
print "L2 norm: %s" % sum(differences)

# Plot it!
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(actual_x, actual_loss, label="actual")
ax.plot(predicted_x, predicted_loss, label="predicted")
ax.set_xlabel("Iteration")
ax.set_ylabel("Loss")
ax.set_title(log_file_name)
plt.legend()
plt.savefig(log_file_name.replace("log", "png"))

