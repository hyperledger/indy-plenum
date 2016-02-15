import cProfile
import profile
import pstats
from statistics import stdev, mean
import inspect

def calibrate():
    pr = profile.Profile()
    samples = []
    for i in range(20):
        samples.append(pr.calibrate(100000))
        print("calculated {:02d}: {}".format(i+1, samples[i]))
    print("------------------------------------")
    print("         mean: {}".format(mean(samples)))
    print("std deviation: {}".format(stdev(samples)))


def profile_this(func, *args, **kwargs):
    pr = cProfile.Profile()
    pr.bias = 7.328898416011422e-07  # calculated on Jason's machine using 'calibrate' function; YMMV
    pr.enable()

    r = func(*args, **kwargs)
    pr.disable()
    p = pstats.Stats(pr)
    print("Cumulative top 50")
    p.sort_stats('cumulative').print_stats(500)
    print("Time top 50")
    p.sort_stats('time').print_stats(500)
    return r
