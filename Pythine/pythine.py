import threading
from itertools import islice

__author = 'zhengxu'


class LockedIterator(object):
    #
    #   This code is referenced from
    #       http://stackoverflow.com/questions/1131430/are-generators-threadsafe
    #
    def __init__(self, it):
        self.lock = threading.Lock()
        self.it = it.__iter__()

    def __iter__(self):
        return self

    def next(self):
        self.lock.acquire()
        try:
            return self.it.next()
        finally:
            self.lock.release()


class LockedList(list):
    def __init__(self, *args, **kwargs):
        self.lock = threading.Lock()
        super(self.__class__, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        self.lock.acquire()
        try:
            super(self.__class__, self).__setitem__(key, value)
        finally:
            self.lock.release()


class ThreadGroup(object):

    def __init__(self, thread_list):
        self._thread_list = thread_list

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def start(self):
        for thread in self._thread_list:
            thread.start()

    def join(self):
        for thread in self._thread_list:
            thread.join()


class Pythine(object):
    @classmethod
    def map(cls, *args, **kwargs):
        def _creator(func):
            return cls(func, *args, **kwargs)
        return _creator

    def __init__(self, func, thread_num=5, auto_partition=False):
        self._func = func
        self._thread_num = thread_num
        self._auto_partition = auto_partition

    def _thread_worker(self, lock_iter, result_map):
        while True:
            try:
                task_index, task_args, task_kwargs = lock_iter.next()
                task_return = self._func(*task_args, **task_kwargs)
                result_map[task_index] = task_return
            except StopIteration:
                return

    @staticmethod
    def _split_input_slice(seq_len, num_split):
        step = (seq_len + num_split - 1) / num_split
        slices = []
        for k in range(num_split):
            begin = int(min(k * step, seq_len))
            end = int(min((k + 1) * step, seq_len))
            if begin == end:
                raise ValueError('Too many slices such that some splits are empty')
            slices.append((begin, end))
        return slices

    # check args and kwargs
    @staticmethod
    def _check_list_args(args, kwargs):
        args_len = set([len(arg) for arg in args if isinstance(arg, list)])
        kwargs_len = set([len(kwargs[k]) for k in kwargs if isinstance(kwargs[k], list)])
        assert len(args_len) <= 1 and len(kwargs_len) <= 1, \
            "All input list should have the same length"
        if len(args_len) and len(kwargs_len):
            assert args_len[0] == kwargs_len[0], "args and kwargs must have same input size"
            seq_len = list(args_len)[0]
        elif len(args_len) or len(kwargs_len):
            seq_len = max((max(args_len or [0]), max(kwargs_len or [0])))
        else:
            seq_len = 1
        # Now we don't do normalization
        return seq_len

    def _get_pythine_args(self, args, kwargs):
        def _fetch_default(key, default):
            if key in kwargs:
                value = kwargs[key]
                del kwargs[key]
            else:
                value = default
            return value

        return {
            'thread_num': _fetch_default('__pythine_thread_num', self._thread_num),
            'auto_partition': _fetch_default('__pythine_auto_partition', self._auto_partition)
        }

    @staticmethod
    def _create_args_iterator(args, kwargs, start, stop):
        arg_list_holder = [i for i, v in enumerate(args) if isinstance(v, list)]
        kwargs_list_holder = [k for k, v in kwargs.iteritems() if isinstance(v, list)]
        out_args = list(args[:])
        out_kwargs = dict(kwargs)
        for seq_idx in range(start, stop):
            for i in arg_list_holder:
                out_args[i] = args[i][seq_idx]
            for k in kwargs_list_holder:
                out_kwargs = kwargs[k][seq_idx]
            yield seq_idx, tuple(out_args), out_kwargs

    def __call__(self, *args, **kwargs):
        # now only supports 1-d list
        pythine_args = self._get_pythine_args(args, kwargs)
        seq_len = self._check_list_args(args, kwargs)
        if seq_len == 1:
            # Make function call transparent if user only give one set of argument
            return self._func(*args, **kwargs)
        else:
            args_iter = self._create_args_iterator(args, kwargs, 0, seq_len)
            result_map = LockedList([None] * seq_len)
            if pythine_args['auto_partition']:
                even_slices = self._split_input_slice(seq_len,
                                                      pythine_args['thread_num'])
                thread_list = [threading.Thread(target=self._thread_worker,
                                                args=(self._create_args_iterator(args, kwargs, start, stop),
                                                      result_map))
                               for start, stop in even_slices]
            else:
                lock_iter = LockedIterator(args_iter)
                thread_list = [threading.Thread(target=self._thread_worker,
                                                args=(lock_iter, result_map))
                               for _ in range(pythine_args['thread_num'])]
            with ThreadGroup(thread_list) as thread_group:
                thread_group.start()
                thread_group.join()
            if not reduce(lambda x, y: x and (not y), result_map, True):
                return result_map


