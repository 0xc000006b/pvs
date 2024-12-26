import hazelcast
import time
import threading

from datetime import datetime


class Hazelcast:
    def __init__(self, type, count=10000, thread_num=10, atomic=False):
        self.client = hazelcast.HazelcastClient(cluster_members=["127.0.0.1:5701","127.0.0.1:5702","127.0.0.1:5703"])
        
        self.threads = []
        self.thread_num = thread_num
        self.count = count
        self.atomic = atomic
        self.map = "counter_map"
        self.key = "counter_key"
        
        if self.atomic:
            self.distributed_map = self.client.cp_subsystem.get_atomic_long(self.key).blocking()
        else:
            self.distributed_map = self.client.get_map(self.map).blocking() 
        
        self.func_names = [
            "increment",
            "optimistic",
            "pessimistic",
            "atomic"
        ]
        self.type = type

    def func_name(self):
        return self.func_names[self.type]

    def get_value(self):
        if self.atomic:
            val = self.distributed_map.get()
        else:
            try:
                val = self.distributed_map.get(self.key)
                if not val:
                    val = 0
            except:
                val = 0
        return val
    
    def set_zero(self):
        try:
            if self.atomic:
                self.distributed_map.set(0)
            else:
                self.distributed_map.put(self.key, 0)
            self.print('counter zero')
            return True
        except:
            return False

    def increment_counter(self):
        for _ in range(self.count):
            counter = self.distributed_map.get(self.key) or 0
            self.distributed_map.put(self.key, counter + 1)

    def optimistic_increment(self):
        for _ in range(self.count):
            while True:
                old_value = self.distributed_map.get(self.key)
                new_value = (old_value or 0) + 1

                if self.distributed_map.replace_if_same(self.key, old_value, new_value):
                    break

    def pessimistic_increment(self):
        for _ in range(self.count):
            self.distributed_map.lock(self.key)
            try:
                counter = self.distributed_map.get(self.key) or 0
                self.distributed_map.put(self.key, counter + 1)
            finally:
                self.distributed_map.unlock(self.key)
    
    def atomic_increment(self):
        for _ in range(self.count):
            self.distributed_map.increment_and_get()
    
    def monitoring(self, timer:int=5):
        while any(thread.is_alive() for thread in self.threads):
            self.get_log()
            time.sleep(timer)

    def get_log(self):
        self.print(f"counter: {self.get_value()}")
    
    def print(self, *args, **largs):
        print(f"{datetime.now().strftime('%H:%M:%S')}  ", *args, **largs)

    def main(self):
        self.set_zero()
        self.get_log()
        funcs = [
            self.increment_counter,
            self.optimistic_increment,
            self.pessimistic_increment,
            self.atomic_increment
        ]

        self.print(f"function: {self.func_name()}")

        start_time = time.time()
        for _ in range(self.thread_num):
            thread = threading.Thread(target=funcs[self.type])
            self.threads.append(thread)
       
        [i.start() for i in self.threads]

        self.monitoring(10)
        self.get_log()
        print()
        self.print(f"{self.func_name()} result:")
        self.print(f"counter: {self.get_value()}")
        self.print(f"time: {int(time.time()-start_time)}s\n\n")


if __name__ == "__main__":
    for i in range(4):
        hc = Hazelcast(i, atomic=i == 3)
        try:
            hc.main()
        finally:
            hc.client.shutdown()
    