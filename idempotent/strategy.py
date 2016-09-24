# -*- coding=utf8 -*-
"""
    实现函数的幂等性
"""
import cPickle
from redis import RedisError


class Strategy(object):
    """幂等接口策略"""

    def __init__(self, func_key, redis_client, expiration_time):
        """ 构造函数

        Args:
            func_key (str): 函数和参数唯一标识
            redis_client (StrictRedis): redis客户端
            expiration_time (int): redis过期时间
        """
        self.func_key = func_key
        self.redis_client = redis_client
        self.expiration_time = expiration_time

        self.step_counter = 0
        self.old_success = False
        self.old_result = None
        self.old_steps = []
        self.new_steps = []
        self.is_redis_available = True

    def begin(self):
        """开始操作"""
        try:
            execute_info = self.redis_client.get(self.func_key)
            if execute_info:
                execute_info = cPickle.loads(execute_info)
                self.old_success = execute_info.get('suc', False)
                self.old_result = execute_info.get('rl')
                self.old_steps = execute_info.get('stp', [])
        except RedisError:
            self.is_redis_available = False
        except cPickle.PickleError:
            pass

    def add(self, fn, args=None, kwargs=None):
        """添加步骤

        Args:
            fn (function): 可执行对象
            args (list): 列表参数
            kwargs (dict): 字典参数
        """
        args = args or []
        kwargs = kwargs or {}
        if not self.is_redis_available:
            return fn(*args, **kwargs)

        if self.step_counter < len(self.old_steps):
            result = self.old_steps[self.step_counter]
        else:
            result = fn(*args, **kwargs)

        self.new_steps.append(result)
        self.step_counter += 1
        return result

    def end(self, suc, rl):
        """完成操作

        Args:
            suc (bool): 整个过程执行是否有错
            rl (object): 执行的结果
        """
        result = {
            'suc': suc,
            'rl': rl,
            'stp': self.new_steps
        }
        self.redis_client.set(self.func_key, cPickle.dumps(result))
        self.redis_client.expire(self.func_key, self.expiration_time)
