# -*- coding=utf8 -*-
"""
    示例
"""
from redis import StrictRedis
from idempotent.decorator import idem


redis_client = StrictRedis.from_url('redis://127.0.0.1/1')


def func1():
    return 5


def func2(a):
    import random
    ran = random.randint(1, 3)
    if ran == 2:
        raise Exception('test')
    return ran


@idem(redis_client, 'test', 60)
def test(strategy, a1, a2):
    b = strategy.add(func1)
    print strategy.add(func2, args=[b])


test(None, 2, 6)
