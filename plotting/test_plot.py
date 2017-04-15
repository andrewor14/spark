#!/usr/bin/env python

import math
import matplotlib.pyplot as plt

def one_over_x(x, a, b): return 1 / (a * x + b)
def one_over_x_squared(x, a, b, c): return 1 / (a * math.pow(x, 2) + b * x + c)
def one_over_x_to_the_k(x, a, b, k): return 1 / (a * math.pow(x, k) + b)

def plot(call_func, x, y, nice_string, *params):
  fitted_y = [call_func(xx) for xx in x]
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(x, y, "x", label="orig")
  ax.plot(x, fitted_y, label="fitted")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Loss")
  ax.text(10, 0.75, nice_string)
  plt.savefig("output.png")

def main():
  (a, b, k) = (0.8504322922809595, 0.9692050016608997, 0.2553913814050604)
  x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200]
  y = [0.8457629079679162, 0.6933319467840048, 0.6929244830481034, 0.6928985266935901, 0.6927571425168353, 0.6924604412401963, 0.6916249078058486, 0.6895895498963561, 0.6846972055207718, 0.6748453455664715, 0.6615279118265535, 0.6406061298508541, 0.6195759433440048, 0.6074961881991002, 0.581231992825909, 0.5743819481862534, 0.56489699801493, 0.5627604370224314, 0.5594615301308806, 0.5401119702587239, 0.5286143460109968, 0.518120144901153, 0.5143282641898178, 0.48420039511402907, 0.4497209659492151, 0.43202219344379034, 0.40990069298675774, 0.3981713198455558, 0.3884416791000582, 0.3798003396701819, 0.37430043003635116, 0.37329480135290705, 0.3669801430027825, 0.3628883587392117, 0.3608324403737805, 0.3571956152174816, 0.35626391328229345, 0.3511834843340789, 0.3485329745828947, 0.34548754474699606, 0.3434731639545161, 0.3338718231187778, 0.32492851590874705, 0.31932624330878673, 0.3154118275422862, 0.31207797510532176, 0.310263766336573, 0.3098614662128361, 0.30935679777384334, 0.30729408200488034, 0.3047749174771877, 0.30388731313722295, 0.30281259535462335, 0.3021952750485106, 0.30017047308760786, 0.29917180801017185, 0.29855723852160565, 0.29735657181013997, 0.29531156259643326, 0.28954840164563395, 0.2867203149238804, 0.2857845288188469, 0.28531651281896936, 0.28410726371641715, 0.2828583193961582, 0.28074127750722566, 0.27763223297189654, 0.27566229616076243, 0.2751318485598475, 0.2744916944202083, 0.2742588938133108, 0.27348121727358354, 0.27297709935678927, 0.2716371005137619, 0.27138609030636124, 0.27117880936928096, 0.2709394135815537, 0.27021014032622404, 0.26949647440219804, 0.2683052552899231, 0.2669332738064714, 0.2657604358498971, 0.2648578347900865, 0.26385815865828144, 0.2630958674013494, 0.26239177419965104, 0.2621643801046664, 0.261360447091009, 0.2610923041720835, 0.26053485210182586, 0.26004355583457905, 0.25954472517562843, 0.2591329048595771, 0.25880010215964144, 0.2583149888535026, 0.25773375403556953, 0.2572059530258111, 0.2570840496549884, 0.2568498361478914, 0.2566092982357517, 0.2556086234826002, 0.25497498457210566, 0.25450090329084174, 0.25410602924379405, 0.2537472647349707, 0.2535459677807628, 0.25324296687044184, 0.2530755774590491, 0.2527857173104992, 0.25252978563019374, 0.2524057859356217, 0.25216717817595735, 0.25184628425964606, 0.25154052358957424, 0.25141562639429194, 0.25104530036484496, 0.25091126499491, 0.2506668803110683, 0.2503218300162089, 0.24998797528854286, 0.24934824209486525, 0.2488547615229993, 0.24838598559128472, 0.2481231474228046, 0.24787360287354382, 0.24769030142018955, 0.24743741864032076, 0.24735373104328656, 0.2472369579904753, 0.2470457505160937, 0.2466018013365057, 0.24608850783873282, 0.24561873099177733, 0.24543265224011504, 0.2451848785620283, 0.24502332523559237, 0.2449066635603593, 0.24485915091144408, 0.24479338842517825, 0.24473271802306376, 0.24463170232129297, 0.24448596339227785, 0.2442015538026468, 0.24355526048388557, 0.24318151975781302, 0.24283853826777096, 0.24265602971200473, 0.24254342128143325, 0.24242190740091762, 0.2421750247356944, 0.24188227359508868, 0.24149675284109123, 0.24140781866556316, 0.2413542616729172, 0.2413422661905294, 0.24127473117839526, 0.24118339644532566, 0.24110350883133355, 0.2410266076947452, 0.24099994800358235, 0.24094283622431822, 0.24090840323945345, 0.24077422119908212, 0.24064852650020227, 0.24059733261796568, 0.2404678209881306, 0.24045469778133788, 0.2404313754195613, 0.24039962746287843, 0.2402863442330123, 0.24023249056363416, 0.24009492106763164, 0.23998952350095773, 0.23993230118397274, 0.23979711907909723, 0.2396189095906243, 0.2396044299924791, 0.23952759970190293, 0.23937721959813874, 0.23912089063681166, 0.2391070267314608, 0.23899717566478135, 0.23895708512750374, 0.23893383984159808, 0.2389060872900614, 0.23885567644833886, 0.23884015076815313, 0.23878092059539857, 0.2387701706185684, 0.23874615990706172, 0.2387350698168314, 0.2387144160873143, 0.23867643996283344, 0.23862630339118493, 0.23855989886691356, 0.23851316277817328, 0.23845262583753915, 0.23825268092188134, 0.23821328673039324, 0.23815637606293943]
  call_func = lambda x: one_over_x_to_the_k(x, a, b, k)
  nice_string = "1 / (ax^k + b)\na = %s\nb = %s\nk = %s" % (a, b, k)
  plot(call_func, x, y, nice_string, a, b, k)

if __name__ == "__main__":
  main()

