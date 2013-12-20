#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: xudb
# date: 2013-9-18
# description: 数据库相关操作

from pysqlite2 import dbapi2 as sqlite3
import logging
logger = logging.getLogger(__name__)

class Sqlite3DB(object):
    """ sqlite3 数据库相关操作，初始化 和 数据插入"""

    def __init__(self, db_file):
        """ 初始化数据库，新建连接，新建数据库表"""
        # check_same_thread 设置为 True ， 只在 主线程里进行写入操作。
        # 多线程写入的话会造成 DatabaseError: database disk image is malformed
        self.conn = sqlite3.connect(db_file, isolation_level=None,
                                    check_same_thread = True)
        self.conn .text_factory = str
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS
        spider_result (
            id integer primary key autoincrement,
            url varchar(256),
            title varchar(128),
            depth smallint,
            content text,
            keyword varchar(256))""")


    def save_data(self, page_info):
        """ 数据库插入"""
        if self.conn:
            # logger.error(len(page_info['content']))
            page_info_tuple = (page_info['url'], page_info['title'],
                               page_info['depth'], page_info['content'],
                               page_info['keyword']
            )
            self.conn.execute("insert into spider_result (url, title, depth, content, keyword) values (?, ?, ?, ?, ?)", page_info_tuple)
        else:
            print 'Database error!'


