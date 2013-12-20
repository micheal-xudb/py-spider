#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: xudb
# date: 2013-9-18
# description: 爬虫主要逻辑

import logging
import time
import Queue
from urlparse import urlparse
import page_handler
from thread_pool import make_tasks, ThreadPoolManager, NoResultsPending

logger = logging.getLogger(__name__)


class Spider(object):
    """ 爬虫主程序
        主要功能：通过 requests 下载链接获得网页源代码，通过beautifulsoup 分析网页源代码，
                提取出其中的链接，并做关键字过滤。提取出的链接放入到待访问的列表中，下一深度
                进行访问。
    """
    def __init__(self, url, depth, thread_num, key, down_file, database):
        self.depth = depth   # 爬虫爬取深度
        self.current_depth = 1 # 爬虫当前爬取深度
        self.keyword = key  # 爬取关键字
        self.database = database
        self.task_pool_manager = ThreadPoolManager(thread_num) #线程池
        self.visited_hrefs = set()  # 爬虫已经访问的链接
        self.unvisited_hrefs = set() # 爬虫在某一深度将要访问的链接
        self.html_queue = Queue.Queue()
        self.unvisited_hrefs.add(url)
        self.down_file_type = down_file


    def start(self):
        print "\n*****Start Crawling*****\n"
        time_start = time.time()
        if not self.database.conn:
            print 'Error: Unable to open database file.\n'
        else:
            while self.current_depth < self.depth + 1:
                self.add_current_depth_tasks() # 任务添加到线程池
                self.task_pool_manager.wait()
                self.store_page()
                logger.warning('深度 %d 完成，共访问 %d 个链接 \r\n 下个深度将访问链接数 %s'
                            % (self.current_depth, len(self.visited_hrefs),
                               len(self.unvisited_hrefs)))
                self.current_depth += 1
            self.stop()
        print "\n***** Stop Crawling *****\n"
        print "\n 共耗时： %s 秒\n" % (time.time() - time_start)

    def add_current_depth_tasks(self):
        """ 将当前深度需要访问的链接制作为 线程池可以处理的 Task，把所有Task加入到线程池中。"""
        spider_tasks = make_tasks(self.spider_task_handler,
                                 self.unvisited_hrefs)
        for spider_task in spider_tasks:
            self.task_pool_manager.add_task(spider_task)
        self.unvisited_hrefs = set()
        while True:
            try:
                time.sleep(0.5)
                self.task_pool_manager.poll()
            except NoResultsPending:
                break

    def spider_task_handler(self, url):
        """获取页面，保存页面中的链接并添加到 unvisited_hrefs"""
        page = page_handler.Page(url)
        if page.get_page_source():
            self.visited_hrefs.add(url)
            self.add_unvisited_urls(page.get_page_urls())
            page.save_page_info(self.keyword, self.html_queue,
                                self.current_depth, self.down_file_type)

    def add_unvisited_urls(self, hrefs):
        """ 把页面中未访问的链接添加到 unvisited_hrefs"""
        for href in hrefs:
            protocal = urlparse(href).scheme
            if (protocal == 'http' or protocal == 'https'):
                if not (href in self.unvisited_hrefs or href in self.visited_hrefs):
                    self.unvisited_hrefs.add(href)

    def store_page(self):
        """ 完成某一深度的爬取，把 html_queue 里的数据保存到数据库中。
            如果当前深度数据量过大可能会有问题－后续需要当 html_queue 达到一定大小时开始写入数据库
            或者不用 sqlite 。
        """
        while not self.html_queue.empty():
            page_info = self.html_queue.get()
            self.database.save_data(page_info)

    def stop(self):
        """ 停止爬虫，把所有线程池里的任务结束并且等待结果返回。"""
        self.task_pool_manager.dismiss_workers(
            self.task_pool_manager.thread_num, True)
        self.database.conn.close()
