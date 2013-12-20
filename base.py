#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: xudb
# date: 2013-9-18
# description: 底层功能

import logging
import argparse
import ConfigParser
import requests
from pysqlite2 import dbapi2 as sqlite3

def get_arg():
    """ 获取命令行参数，参数的默认值从 config 文件中获取，或者在本文件中指定
        返回 namespace args
    """
    config_parser = ConfigParser.ConfigParser()
    config_parser.read("config")
    try:
        option_config = dict(config_parser.items("options"))
        default_arg = {
            "log_file": option_config.get("log_file"),
            "log_level": option_config.get("log_level"),
            "thread_num": option_config.get("thread_num"),
            "db_file": option_config.get("db_file"),
            "key_word": option_config.get("key_word"),
            "down_file": option_config.get("down_file")
        }
    except Exception:
        default_arg = {
            "log_file": "spider.log",
            "log_level": 3,
            "thread_num": 10,
            "db_file": "spider.sql",
            "key_word": "",
            "down_file": ""
        }
    parser = argparse.ArgumentParser(description="A web spider")
    parser.add_argument("-u", type=str, required=True, metavar="URL",
                        dest="url", help="The start url of the spider.")
    parser.add_argument("-d", type=int, required=True, metavar="DEPTH",
                        dest="depth", help="The depth of the spider crawl.")
    parser.add_argument("-f", type=str, metavar="FILE", default=default_arg["log_file"],
                        dest="log_file", help="The log file for the spider.")
    parser.add_argument("-l", type=int, choices=[1, 2, 3, 4, 5],
                        default=default_arg["log_level"], dest="log_level",
                        help="The level of the log file, default 3.")
    parser.add_argument("--testself", action="store_true", dest="test_self",
                        help="Test the spider")
    parser.add_argument("--thread", type=int, metavar="NUM",
                        default=default_arg["thread_num"], dest="thread_num",
                        help="The size of the thread pool.")
    parser.add_argument("--dbfile", type=str, metavar="FILE", dest="db_file",
                        default=default_arg["db_file"], help="File to store pages")
    parser.add_argument("--key", type=str, metavar="KEY", dest="key_word",
                        default=default_arg["key_word"], help="The key word for crawl")
    parser.add_argument("--downloadfile", type=str, metavar="FILE", dest="down_file",
                        default=default_arg["down_file"], help="The file to download.")
    return parser.parse_args()

def check_args(args):
    """ 参数验证，验证 thread_num 大于 0"""
    if args.thread_num < 1:
        print 'Thread num must larger than 0'
        return False
    return True

def handle_args(args):
    """ 参数处理，处理 url 和 down_file"""
    if not args.url.startswith('http://') or args.url.startswith('https://'):
        print 'Url not start with http://'
        args.url = "http://" + args.url
    if args.down_file:
        file_type_list = args.down_file.split(';')
        args.down_file = file_type_list

def set_logger(log_file, log_level):
    """ 设置logger对象
        Args: log_file 日志存储文件
              log_level 日志级别
        Returns: True 设置成功
                 False 设置失败
    """
    log_format = "%(asctime)-15s %(levelname)s %(name)s %(message)s"
    level_map = {
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG}
    try:
        logging.basicConfig(filename=log_file, level=level_map.get(log_level),
                           format=log_format)
        return True
    except:
        return False

def test_self():
    """ 程序自检，检查网络状态是否正常，数据库写入是否正常。
        后续可以加入线程池的管理是否正常。
    """
    test_url = r"http://www.w3school.com.cn"
    resp = requests.get(test_url, timeout=5, stream=True)
    if resp.status_code == requests.codes.ok and len(resp.text) > 1:
        print r"网络连接正常！"
    else:
        print r"网络连接异常！"

    test_db_file = "test.db"
    try:
        conn = sqlite3.connect(test_db_file, isolation_level=None,
                               check_same_thread= True)
        conn.text_factory = str
        conn.execute("""
            CREATE TABLE IF NOT EXISTS
            spider_result (
                id integer primary key autoincrement,
                url varchar(256),
                title varchar(128),
                depth smallint,
                content text,
                keyword varchar(256))""")
        conn.execute("insert into spider_result (url, title, depth, content, keyword) values (1, 1, 1, 1, 1);")
    except:
        print r"数据库写入异常！"
    else:
        print r"数据库写入正常！"

