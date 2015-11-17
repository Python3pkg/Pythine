
from Pythine.deprecated import PythineDeprecated
from Pythine import Pythine

import time
import math


@Pythine.map(thread_num=5)
def do_something_heavy_iter(i):
    for _ in range(50):
        value = math.exp(math.log(i))
    return value


@PythineDeprecated.map(thread_num=5)
def do_something_heavy_list(i):
    for _ in range(50):
        value = math.exp(math.log(i))
    return value


def benchmark_iter_argslist(test_time):
    test_list = list(range(1, 1+test_time))
    t0 = time.time()
    ret1 = do_something_heavy_list(test_list)
    t1 = time.time()
    ret2 = do_something_heavy_iter(test_list)
    t2 = time.time()
    ret3 = do_something_heavy_iter(test_list, __pythine_auto_partition=True)
    t3 = time.time()
    print "ArgList Engine: %f" % (t1-t0)
    print "ArgIter Engine: %f" % (t2-t1)
    print "ArgIter Engine(auto partition): %f" % (t3-t2)
    pass

if __name__ == '__main__':
    benchmark_iter_argslist(5000)


