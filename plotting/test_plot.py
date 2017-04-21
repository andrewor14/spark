#!/usr/bin/env python

import math
import matplotlib.pyplot as plt

def main():
  x = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0, 75.0, 76.0, 77.0, 78.0, 79.0]
  y = [1.207537312, 1.185292176, 1.166618175, 1.150936904, 1.135723341, 1.121411389, 1.107769733, 1.094854071, 1.083197946, 1.071859239, 1.06143818, 1.051750085, 1.042143654, 1.033183938, 1.024658228, 1.016700305, 1.008686881, 1.000729208, 0.9933261475, 0.986518416, 0.9799142534, 0.9732812452, 0.9669945793, 0.9606147749, 0.9546116701, 0.9489511013, 0.9434551049, 0.9381606316, 0.9331822599, 0.9281916679, 0.9228515538, 0.9179884758, 0.9130313544, 0.9084737224, 0.9037394192, 0.8990305878, 0.8943156112, 0.8901588707, 0.8859570957, 0.8818234209, 0.8776390252, 0.8737290495, 0.8700639719, 0.8663850582, 0.8627729522, 0.8592213716, 0.8556776871, 0.8525707286, 0.8490569123, 0.8458280029, 0.8424632555, 0.8390222543, 0.8358797038, 0.8329674252, 0.8299563512, 0.8271323388, 0.8242646104, 0.8211750171, 0.8183365251, 0.815615492, 0.8131757107, 0.8105198511, 0.807756987, 0.8050878578, 0.8024759983, 0.8000067534, 0.7974863165, 0.7950453804, 0.7925165678, 0.7899536673, 0.7877225014, 0.7855323884, 0.7831206625, 0.7809092324, 0.7785599208, 0.7762259524, 0.7740861791, 0.7721399546, 0.770117015, 0.7680678973]
  # grep actual sim_mlpc_cf_OneOverXSquaredFunctionFitter_0.5.log | grep "ANDREW(5[1-5])" | sed 's/.* = \([0-9.]*\))/\1/g' | tr '\n' ',' | sed 's/,/, /g'
  extra_x = range(80, 90)
  extra_y = [0.7659377658, 0.7639118497, 0.7618474866, 0.7599556587, 0.7581044124, 0.7562040952, 0.7540958372, 0.7522664195, 0.7505031692, 0.7486026983]
  (a, b) = (0.003803654596574092, 1.0023444061625462)
  plot_one_over_x(x, y, extra_x, extra_y, a, b)
  #(a, b, c, d) = (-2956.6876697900725, 109333.03770370275, -10666.18169301841, 0.8491570973191973)
  #plot_one_over_x_squared(x, y, extra_x, extra_y, a, b, c, d)

def plot(call_func, x, y, extra_x, extra_y, nice_string, *params):
  x = x[50:]; y = y[50:]
  fitted_y = [call_func(xx) for xx in x + extra_x]
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(x, y, "x", label="orig")
  ax.plot(x + extra_x, fitted_y, label="fitted")
  ax.plot(extra_x, extra_y, "x", label="future")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Loss")
  ax.text(10, 0.75, nice_string)
  plt.savefig("output.png")

def one_over_x(x, a, b): return 1 / (a * x + b)

def one_over_x_squared(x, a, b, c, d): return 1 / (a * math.pow(x + b, 2) + c) + d

def one_over_x_to_the_k(x, a, b, c, k): return 1 / (a * math.pow(x, k) + b) + c

def one_over_exponential(x, a, b): return math.exp(-1 * a * x) + b

def plot_one_over_x(x, y, extra_x, extra_y, a, b):
  call_func = lambda x: one_over_x(x, a, b)
  nice_string = "1 / (ax + b)\na = %s\nb = %s" % (a, b)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b)

def plot_one_over_x_squared(x, y, extra_x, extra_y, a, b, c, d):
  call_func = lambda x: one_over_x_squared(x, a, b, c, d)
  nice_string = "1 / (a(x + b)^2 + c) + d\na = %s\nb = %s\nc = %s\nd = %s" % (a, b, c, d)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b, c, d)

def plot_one_over_x_to_the_k(x, y, extra_x, extra_y, a, b, c, k):
  call_func = lambda x: one_over_x_to_the_k(x, a, b, c, k)
  nice_string = "1 / (ax^k + b) + c\na = %s\nb = %s\nc = %s\nk = %s" % (a, b, c, k)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b, c, k)

def plot_one_over_exponential(x, y, extra_x, extra_y, a, b):
  call_func = lambda x: one_over_exponential(x, a, b)
  nice_string = "exp(-ax) + b\na = %s\nb = %s" % (a, b)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b)

if __name__ == "__main__":
  main()

