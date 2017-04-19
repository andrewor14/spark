#!/usr/bin/env python

import math
import matplotlib.pyplot as plt

def main():
  x = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
  y = [0.8298160792195828, 0.6925252153913596, 0.6920100499363546, 0.6919793192876244, 0.691852212612796, 0.6915696186839926, 0.69081602037653, 0.689080327461678, 0.6855736625087322, 0.6807537655511823, 0.667048570262496, 0.6381420927175953, 0.6218858965335436, 0.5940733914315439, 0.5718304288280405, 0.5713272492170728, 0.5689950323429458, 0.5622575156833408, 0.547539385582486, 0.5207554554907012]
  extra_x = [20.0, 21.0, 22.0, 23.0, 24.0]
  extra_y = [0.4573655040665938, 0.39099156769153515, 0.3728753789983046, 0.35993838961343394, 0.3592222900749839]
  (a, b) = (1.9662751211607439, 0.5880586467638157)
  #(a, b, c) = (6.382963531318957E-10, 2.3436749568810208E-4, -4266.047899739164)
  plot_one_over_exponential(x, y, extra_x, extra_y, a, b)
  #plot_one_over_x(x, y, extra_x, extra_y, a, b, c)

def plot(call_func, x, y, extra_x, extra_y, nice_string, *params):
  #x = x[10:]; y = y[10:]
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

def one_over_x(x, a, b, c): return 1 / (a * x + b) + c

def one_over_x_squared(x, a, b, c, d): return 1 / (a * math.pow(x, 2) + b * x + c) + d

def one_over_x_to_the_k(x, a, b, c, k): return 1 / (a * math.pow(x, k) + b) + c

def one_over_exponential(x, a, b): return math.exp(-1 * a * x) + b

def plot_one_over_x(x, y, extra_x, extra_y, a, b, c):
  call_func = lambda x: one_over_x(x, a, b, c)
  nice_string = "1 / (ax + b) + c\na = %s\nb = %s\nc = %s" % (a, b, c)
  plot(call_func, x, y, extra_x, extra_y, nice_string, a, b, c)

def plot_one_over_x_squared(x, y, extra_x, extra_y, a, b, c, d):
  call_func = lambda x: one_over_x_squared(x, a, b, c, d)
  nice_string = "1 / (ax^2 + bx + c) + d\na = %s\nb = %s\nc = %s\nd = %s" % (a, b, c, d)
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

