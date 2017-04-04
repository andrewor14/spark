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
actual_loss = []
predicted_loss = []
with open(log_file_name) as f:
  cool_lines = [line for line in f.readlines() if "ANDREW" in line]
  actual_loss = [None] * (len(cool_lines) * 2)
  predicted_loss = [None] * (len(cool_lines) * 2)
  for line in cool_lines:
    loss = float(line.split()[-1])
    index = int(re.match(".*ANDREW\((.*)\).*", line).groups()[0])
    if "actual" in line:
      actual_loss[index] = loss
    elif "predicted" in line:
      predicted_loss[index] = loss
    else:
      print "Yikes, bad line:\n\t%s" % line
  # Now get rid of None entries
  bad_indices = []
  for i in range(len(actual_loss)):
    if actual_loss[i] is None or predicted_loss[i] is None:
      bad_indices += [i]
  for i in list(reversed(sorted(bad_indices))):
    del actual_loss[i]
    del predicted_loss[i]

# What's the difference between the actual loss and the predicted loss?
differences = [predicted_loss[i] - actual_loss[i] for i in range(len(actual_loss))]
print "Average difference: %s" % (float(sum(differences)) / len(differences))

# Plot it!
x = range(len(actual_loss))
plt.plot(x, actual_loss)
plt.plot(x, predicted_loss)
plt.show()

