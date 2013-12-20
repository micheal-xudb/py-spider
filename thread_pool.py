#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: xudb
# date: 2013-9-18
# description: 线程池 多线程的管理

import logging
import Queue
from threading import Thread, Event

logger = logging.getLogger(__name__)

def make_tasks(func, args_list):
    """ 创建任务队列以便在之后 由线程池里的线程 取出并执行"""
    tasks = []
    for item in args_list:
        tasks.append(TaskRequest(func, [item]))
    return tasks

class NoResultsPending(Exception):
    """ 所有任务都处理完成时抛出"""
    pass

class NoWorkersAvailable(Exception):
    """ 没有可用线程时抛出"""
    pass

class TaskRequest(object):
    """ 线程任务"""
    def __init__(self, func, args=None, kwds=None):
        self.task_id = id(self)
        self.func = func
        self.args = args or []
        self.kwds = kwds or {}

    def __str__(self):
        return "<TaskRequest id=%s args=%r kwargs=%r>" % \
               (self.task_id, self.args, self.kwds)

class Worker(Thread):
    """ 工作线程，取出线程池中的任务，执行任务中指定的函数，完成任务功能"""

    def __init__(self, task_queue, res_queue, poll_timeout=2, **kwds):
        Thread.__init__(self, **kwds)
        logger.debug('Worker Thread id ::  %s', id(self))
        self.setDaemon(True)
        self.task_queue = task_queue
        self.result_queue = res_queue
        self._poll_timeout = poll_timeout
        self._dismissed = Event()
        self.start()

    def run(self):
        """重复取出 task_queue 里的任务并执行，直到通知其退出。"""
        while True:
            if self._dismissed.isSet():
                break
            try:
                task = self.task_queue.get(
                    block=True, timeout=self._poll_timeout)
            except Queue.Empty:
                continue
            else:
                if self._dismissed.isSet():
                    # 线程退出，把请求放回 task_queue
                    self.task_queue.put(task)
                    break
                result = task.func(*task.args, **task.kwds)
                logger.debug('Worker Thread id ::  %s' % id(self))
                self.result_queue.put((task, result))

    def dismiss(self):
        """当前任务结束后，退出线程"""
        self._dismissed.set()


class ThreadPoolManager(object):
    """ 线程池管理
        task_queue 需要处理的任务队列，轮询取出并执行任务。
        result_queue 结果队列
        workers 线程池，存放工作线程，执行任务
    """

    def __init__(self, thread_num):
        self.workers = []
        self.thread_num = thread_num
        self.task_queue = Queue.Queue()
        self.result_queue = Queue.Queue()
        self.dismissed_workers = []
        self.tasks = {}
        self.add_workers(thread_num)
        logger.info('Worker nums %s', len(self.workers))

    def add_workers(self, workers_num, poll_timeout=2):
        """ 向线程池里添加 worker 线程"""
        for i in xrange(workers_num):
            self.workers.append(
                Worker(self.task_queue, self.result_queue,
                       poll_timeout=poll_timeout))

    def dismiss_workers(self, num_workers, do_join=False):
        """ 等待所有 worker 线程结束并退出线程"""
        dismiss_list = []
        for i in xrange(min(num_workers, len(self.workers))):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)

        if do_join:
            for worker in dismiss_list:
                worker.join()
        else:
            self.dismissed_workers.extend(dismiss_list)

    def poll(self, block=False):
        """ 轮询 取出 result_queue 里的结果，从 tasks 中删除已经取出结果的 task"""
        while True:
            if not self.tasks:
                raise NoResultsPending
            elif block and not self.workers:
                raise NoWorkersAvailable
            try:
                task, result = self.result_queue.get(block=block)
                # logger.debug('task---------------%s',task)
                del self.tasks[task.task_id]
            except Queue.Empty:
                break

    def wait(self):
        """ 等待所有线程退出"""
        while True:
            try:
                self.poll(True)
            except NoResultsPending:
                break


    def add_task(self, task, block=True, timeout=None):
        """向 task_queue 加入任务，存储 request_id"""
        assert isinstance(task, TaskRequest)
        self.task_queue.put(task, block, timeout)
        self.tasks[task.task_id] = task