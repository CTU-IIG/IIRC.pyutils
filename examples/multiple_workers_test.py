from multiprocessing import Process, Queue, Value, Lock, Condition
import time
import os
import random

'''
Example how to implement safe multi-process consumer type of application, where consumers may spawn some more work 
based on the result of the current work.
'''

def consumer(queue, lock, working_count, condition, queue_lock):
    with lock:
        print('starting consumer {}'.format(os.getpid()))

    while True:
        queue_lock.acquire()
        condition.wait_for(lambda: not queue.empty() or working_count.value == 0)
        with lock:
            print('working count {}, queue size {}'.format(working_count.value, queue.empty()))

        if not queue.empty():
            name = queue.get()
            with working_count.get_lock():
                working_count.value += 1
            queue_lock.release()

            # do some work
            with lock:
                print('working on {}'.format(name))

            time.sleep(random.random() * 1)  # todo: time consuming operation

            queue_lock.acquire()
            with working_count.get_lock():
                working_count.value -= 1

            # todo: spawn some new work with probability 0.3
            r = random.random()
            if r > 0.7:
                queue.put(r)
                queue.put(r)

            condition.notify_all()
            queue_lock.release()

        elif working_count.value == 0:
            queue_lock.release()
            return
        else:
            continue


if __name__ == '__main__':
    n_proc = 6

    # Some lists with our favorite characters
    work = [['LOFAS', 'losos', 'SPACES'],
             ['krocan', 'kruta', 'kraken'],
             ['a', 'b', 'c', 'd']]

    # inter process queue
    queue = Queue()

    # lock object to synchronize resource access
    lock = Lock()

    queue_lock = Lock()
    condition = Condition(queue_lock)

    # shared-memory counter
    counter = Value('i', 0)

    consumers = []
    for n in work:
        for a in n:
            queue.put(a)

    # create consumer processes
    for i in range(n_proc):
        p = Process(target=consumer, args=(queue, lock, counter, condition, queue_lock))
        consumers.append(p)

    for c in consumers:
        c.start()

    # wait for the workers:
    for c in consumers:
        c.join()

    print('parent process exiting...')
