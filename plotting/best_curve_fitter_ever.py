#!/usr/bin/env python

import re
import sys

from curve_fitter import *


class PredLossStrategy:
  avg = 'avg'
  cf = 'cf'

class CurveFitterName:
  one_over_x = 'one_over_x'
  one_over_x_squared = 'one_over_x_squared'
  exponential = 'exponential'

# Accepted configurations
CONF_PREFIX = "spark.approximation.predLoss"
CONF_STRATEGY = "%s.strategy" % CONF_PREFIX
CONF_WINDOW_SIZE = "%s.windowSize" % CONF_PREFIX
CONF_CF_DECAY = "%s.%s.decay" % (CONF_PREFIX, PredLossStrategy.cf)
CONF_CF_FITTER_NAME = "%s.%s.fitterName" % (CONF_PREFIX, PredLossStrategy.cf)

# Other constants
NUM_ITERATIONS_IN_ADVANCE = 10
MIN_POINTS_FOR_PREDICTION = 5
VERBOSE = True

# All parameters collected for curve fitting so far, one sublist per param
# This is used to compute better initial parameters in case curve fitting fails
all_fitted_params = []

def main():
  args = sys.argv
  if len(args) < 3:
    print "Usage: best_curve_fitter_ever.py <input_file> <output_file> [<additional configs> ...]"
    sys.exit(1)
  loss_file = args[1]
  out_file = args[2]
  conf = parse_additional_confs(args[3:])
  actual_losses = read_losses(loss_file)
  with open(out_file, "w") as f:
    for i in range(len(actual_losses)):
      current_iter = i + 1
      f.write("ANDREW(%s): (actual loss = %.15f)\n" % (current_iter, actual_losses[i]))
      if current_iter >= MIN_POINTS_FOR_PREDICTION:
        predicted_losses = pred_loss(range(1, current_iter + 1), actual_losses[:current_iter], conf)
        for (j, loss) in enumerate(predicted_losses):
          f.write("ANDREW(%s): (predict iter = %s) (predicted loss = %.15f)\n" %\
            (current_iter + j + 1, current_iter, loss))

def parse_additional_confs(args):
  '''
  Parse system arguments that look like "--conf hello=world" into a map.
  '''
  conf = {}
  for i in range(len(args)):
    if args[i] == "--conf" and i + 1 < len(args):
      match = re.match("(.*)=(.*)", args[i + 1])
      if match is not None:
        groups = match.groups()
        assert len(groups) == 2
        conf[groups[0]] = groups[1]
  return conf

def read_losses(input_file):
  '''
  Read in losses from a file, one per line.
  '''
  losses = []
  with open(input_file) as f:
    losses = [float(l) for l in f.readlines()]
  return losses

def pred_loss(x, y, conf):
  '''
  Predict N losses in the future given existing losses.
  '''
  assert len(x) == len(y)
  assert len(x) >= MIN_POINTS_FOR_PREDICTION
  strategy = conf.get(CONF_STRATEGY) or PredLossStrategy.avg
  window_size = max(int(conf.get(CONF_WINDOW_SIZE)) or 100, 2)
  if strategy == PredLossStrategy.avg:
    # Use average delta in current window to extrapolate
    x = x[-window_size:]
    y = y[-window_size:]
    deltas = [y[i] - y[i - 1] for i in range(1, len(y))]
    avg_delta = float(sum(deltas)) / len(deltas)
    return [y[-1] + i * avg_delta for i in range(1, NUM_ITERATIONS_IN_ADVANCE + 1)]
  elif strategy == PredLossStrategy.cf:
    # Fit curve to points in current window to extrapolate
    decay = float(conf.get(CONF_CF_DECAY)) or 0.8
    fitter = conf.get(CONF_CF_FITTER_NAME) or CurveFitterName.one_over_x_squared
    return pred_loss_cf(x, y, window_size, decay, fitter)
  else:
    raise ValueError("Unknown loss prediction strategy: %s" % strategy)

def pred_loss_cf(x, y, window_size, decay, fitter):
  '''
  Helper method to predict future loss with curve fitting.

  In the event of curve fitting failures, we try again different starting parameters,
  decays and window sizes.
  '''
  result = []
  orig_x = x
  orig_y = y
  x = x[-window_size:]
  y = y[-window_size:]
  current_iter = x[-1]
  fail_msg = "WARNING: curve fitting failed at iteration %s. Trying again with" % current_iter
  starting_params = [sum(params) / len(params) for params in all_fitted_params]
  # If we don't have enough points yet, do interpolation to get a better fit
  if len(x) < MIN_POINTS_FOR_CURVE_FITTING:
    (x, y) = interpolate(x, y)
  if VERBOSE:
    print "==================================================================================="
    print "ANDREW predicting in iteration %s" % current_iter
    print "My x's are: %s" % ", ".join([str(t) for t in x])
    print "My y's are: %s" % ", ".join([str(t) for t in y])

  def attempt1():
    return fit_curve(fitter, x, y, decay, verbose=False)
  def attempt2():
    print "%s starting params %s." % (fail_msg, starting_params)
    return fit_curve(fitter, x, y, decay, starting_params, verbose=False)
  def attempt3():
    print "%s decay 1 and starting params %s." % (fail_msg, starting_params)
    return fit_curve(fitter, x, y, 1, starting_params, verbose=False)
  def attempt4(new_window_size = window_size):
    while True:
      new_window_size *= 2
      print "%s decay 1, starting params %s, and window size %s." %\
        (fail_msg, starting_params, new_window_size)
      try:
        x = orig_x[-new_window_size:]
        y = orig_y[-new_window_size:]
        return fit_curve(fitter, x, y, 1, starting_params, verbose=False)
      except:
        if new_window_size >= len(orig_x):
          raise
  fitted_params = try_except(attempt1,\
    lambda: try_except(attempt2,\
      lambda: try_except(attempt3, attempt4)))
  # Keep track of fitted params
  for i in range(len(fitted_params)):
    if i >= len(all_fitted_params):
      all_fitted_params.append([])
    all_fitted_params[i] += [fitted_params[i]]
  # Log debug info
  if VERBOSE:
    print "My params are %s" % ", ".join([str(t) for t in fitted_params])
    print "==================================================================================="
    print "\n\n\n"
  # Compute future points
  return [get_func(fitter)(current_iter + i, *fitted_params)\
    for i in range(1, NUM_ITERATIONS_IN_ADVANCE + 1)]

def try_except(attempt, fail):
  '''
  Helper method to do try except in one line.
  '''
  try:
    return attempt()
  except:
    return fail()

if __name__ == "__main__":
  main()

