import time

import requests

from Pythine.pythine import Pythine
from functools import reduce

__author__ = 'zhengxu'


@Pythine.map(thread_num=5)
def slow_network(url, timeout=1):
    return requests.get(url, timeout=timeout).text


def benchmark_slow_network(test_time):
    url = 'http://www.beibei.com'
    t0 = time.time()
    for _ in range(test_time):
        slow_network(url)
    t1 = time.time()
    ret = slow_network([url] * test_time, __pythine_thread_num=8)
    t2 = time.time()
    print("Without thread engine: %f" % (t1-t0))
    print("With thread engine: %f" % (t2-t1))
    # check result
    assert reduce(lambda x, y: x and bool(y), ret, True), "Some error occured!"
    pass


def transparent_call():
    url = 'http://www.baidu.com'
    t0 = time.time()
    ret = slow_network(url)
    t1 = time.time()
    print("With thread engine but transparent call: %f" % (t1-t0))
    assert bool(ret), "No output"
    pass

if __name__ == '__main__':
    transparent_call()
    benchmark_slow_network(8)

