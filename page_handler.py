#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: xudb
# date: 2013-9-22
# description: 页面处理类，下载页面源文件并解析出里面的url

import requests
import re
import logging
from urlparse import urljoin
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class Page(object):

    def __init__(self, url):
        self.url = url
        self.html_code = None
        self.page_info = {}

    def get_page_source(self, retry=2):
        """ 获取页面源码。"""
        try:
            # time_s = time.time()
            response = requests.get(self.url, timeout=5, stream=True)

            if response.status_code == requests.codes.ok:
                self.html_code = response.text
                # print "use time %s" % (time.time() - time_s)
                return True
            else:
                logger.warning('Page not avaliable. Status code:%d URL: %s \n'
                             % (response.status_code, self.url) )
        except requests.Timeout:
            if retry > 0:
                return self.get_page_source( retry - 1 )
            else:
                logger.exception(' URL: %s \n' % self.url)
        except Exception:
            logger.exception('unkown Exception URL: %s \n' % self.url)
        return None

    def get_page_urls(self):
        """解析html 代码，获取页面里所有链接。返回链接列表"""
        hrefs = []
        soup = None
        try:
            #soup = BeautifulSoup(self.html_code, "html5lib") 影响性能
            soup = BeautifulSoup(self.html_code)
        except BeautifulSoup.HTMLParseError, e:
            logger.exception(e)
        if soup:
            links = soup.find_all('a',href=True)
            for a in links:
                href = a.get('href').encode('utf8')
                if not href.startswith('http'):
                    href = urljoin(self.url, href)
                hrefs.append(href)
            if soup.title:
                self.page_info["title"] = soup.title.string
            else:
                self.page_info["title"] = ''
        return hrefs

    def save_page_info(self, keyword, html_queue, current_depth, down_file_type):
        """ 存储页面信息到 html_queue，一层结束后存到数据库"""
        try:
            self.page_info["keyword"] = keyword
            self.page_info["url"] = self.url
            self.page_info["depth"] = current_depth
            self.page_info["content"] = self.html_code

            if self.check_download_type(down_file_type):
                if keyword:
                    if re.search(keyword, self.html_code, re.I):
                        html_queue.put(self.page_info)
                else:
                    html_queue.put(self.page_info)
            else:
                logger.info('Page ignore download %s', self.url)
        except Exception, e:
            logger.exception(' URL: %s ' % self.url )

    def check_download_type(self, file_type):
        """ 检查是否需要下载该链接"""
        if not file_type or not isinstance( file_type, list):
            return True
        for file_type in file_type:
            if self.url.endswith(file_type):
                return True
        return False