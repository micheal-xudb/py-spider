#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: xudb
# date: 2013-9-18
# description: 程序入口

import logging
import time
import sys
from threading import Thread
import base
from spider import Spider
from database import Sqlite3DB

logger = logging.getLogger(__name__)

class MainThread(Thread):
    """ 打印进度信息
    """
    def __init__(self, spider):
        Thread.__init__(self)
        self.spider = spider
        self.daemon = True

    def run(self):
        while True:
            time.sleep(10)
            print '-' * 20
            print 'Spider已访问链接数 %d' % len(self.spider.visited_hrefs)
            print '当前访问深度 %d' % self.spider.current_depth
            print '待访问链接数 %d' % len(self.spider.unvisited_hrefs)
            print '-' * 20 + '\r\n'

def main():
    """ 程序主入口
        获取命令行参数并做判断和处理，根据参数设置logger，创建线程池和 spider，线程池中加入
        工作线程 处理线程任务，spider向线程池中加入任务。
    """
    # 获取命令行参数并处理
    args = base.get_arg()
    if not base.check_args(args):
        print 'Args error!'
        sys.exit()
    base.handle_args(args)

    # 设置logger
    if not base.set_logger(args.log_file, args.log_level):
        print 'Set logger error'
        sys.exit()
    logger.debug('Get args :%s' % args)

    # 程序自检
    if args.test_self:
        base.test_self()
        sys.exit()

    database =  Sqlite3DB(args.db_file)

    # 创建 spider 和 线程池。根据 thread_num 向线程池加入多个工作线程。
    # 在 spider 中建立多个任务 放入到线程池中。
    spider = Spider(args.url, args.depth, args.thread_num, args.key_word,
                    args.down_file, database)
    main_thread = MainThread(spider)
    main_thread.start()
    spider.start()

if __name__ == "__main__":
    main()
