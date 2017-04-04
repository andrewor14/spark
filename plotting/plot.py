#!/usr/bin/env python

import matplotlib.pyplot as plt
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
  cool_lines = [line for line in f.readlines() if "ANDREW" in line]
  for line in cool_lines:
    loss = float(line.split()[-1])
    time = int(re.match(".*ANDREW\((.*)\).*", line).groups()[0])
    if "actual" in line:
      actual_x += [time]
      actual_loss += [loss]
    elif "predicted" in line:
      predicted_x += [time]
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
    differences += [pred_loss - act_loss]
print "Average difference: %s" % (float(sum(differences)) / len(differences))

# Plot it!
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(actual_x, actual_loss, label="actual")
ax.plot(predicted_x, predicted_loss, label="predicted")
ax.set_xlabel("Time (s)")
ax.set_ylabel("Loss")
ax.set_title(log_file_name)
plt.legend()
plt.show()

