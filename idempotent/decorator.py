# -*- coding=utf8 -*-
"""
    幂等装饰器
"""
import inspect
import functools
from strategy import Strategy


def function_key_generator(namespace, fn):
    """参考dogpile代码"""

    if namespace is None:
        namespace = '%s:%s' % (fn.__module__, fn.__name__)
    else:
        namespace = '%s:%s|%s' % (fn.__module__, fn.__name__, namespace)

    args = inspect.getargspec(fn)
    has_self = args[0] and args[0][0] in ('self', 'cls')

    def generate_key(*args, **kw):
        if kw:
            raise ValueError("function does not accept keyword arguments.")
        if has_self:
            args = args[2:]
        else:
            args = args[1:]

        return namespace + "|" + " ".join(map(str, args))

    generate_key.has_self = has_self
    return generate_key


def idem(redis_client, napespace, expiration_time):
    """幂等装饰器

    Args:
        redis_client (StrictRedis): reids客户端
        napespace (string): 名字空间
        expiration_time (int): 过期时间
    """

    def wrapper(fn):
        key_generator = function_key_generator(napespace, fn)

        @functools.wraps(fn)
        def inner(*args, **kwargs):
            func_key = key_generator(*args, **kwargs)

            strategy = Strategy(func_key, redis_client, expiration_time)
            strategy.begin()

            args = list(args)
            if key_generator.has_self:
                args[1] = strategy
            else:
                args[0] = strategy

            try:
                result = fn(*args, **kwargs)
                strategy.end(True, result)
            except Exception as exc:
                strategy.end(False, exc)
                raise exc
        return inner
    return wrapper
