import time
import requests

from Pythine import Pythine
from functools import reduce


def _slow_network(url):
    return requests.get(url).text


slow_network = Pythine(_slow_network, 8)


def time_slow_network(test_time):
    url = 'http://www.beibei.com/'
    t0 = time.time()
    for _ in range(test_time):
        _slow_network(url)
    t1 = time.time()
    ret = slow_network([url] * test_time)
    t2 = time.time()
    print("Without thread engine: %f" % (t1-t0))
    print("With thread engine: %f" % (t2-t1))
    # check result
    print(reduce(lambda x, y: x and bool(y), ret, True))
    pass

if __name__ == '__main__':
    time_slow_network(8)

