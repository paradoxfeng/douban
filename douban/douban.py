import requests
from requests.exceptions import ConnectionError
from urllib.parse import urlencode
import re
from lxml import etree
import pymongo
from config import *
from multiprocessing import Pool






class Doubanmovies(object):
    def __init__(self):
        self.baseurl = 'https://movie.douban.com/top250?'
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.table = MONGO_TABLE




    def get_index_page(self,num):
        data = {
            'start':num,
            'filter':''
        }
        url = self.baseurl+urlencode(data)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.text
        except ConnectionError:
            print('请求标签页出错')
            return None

    def parse_index_page(self,text):
        pattern = re.compile('<div class="hd">.*?<a href="(.*?)"',re.S)
        detail_urls = re.findall(pattern,text)
        return detail_urls

    def get_detail_page(self,url):
        try:
            response = requests.get(url)
            if response.status_code ==200:
                return response.text
            return None
        except ConnectionError:
            print('请求详情页出错')
            return None


    def parse_detail_page(self,text):
        try:
            html = etree.HTML(text)
        except ValueError:
            print('编译错误')
            return None
        排名 = html.xpath('//span[@class="top250-no"]/text()')[0]
        电影名 = html.xpath('//h1/span[@property="v:itemreviewed"]/text()')[0]
        导演 = html.xpath('//span[@class="attrs"]/a/text()')[0]
        summary = html.xpath('//span[@property="v:summary"]/text()')[0]
        summary = summary.replace('\n','')
        summary = summary.replace('\u3000\u3000','').strip()
        return {
            '排名':排名,
            '电影名':电影名,
            '导演':导演,
            '剧情介绍':summary
        }


    def save_to_mongo(self,data):
        if self.db[self.table].insert(data):
            print('存储成功',data)
            return True
        else:
            return False


    def start(self):
        for x in range(10):
            text = self.get_index_page(x*25)
            urls = self.parse_index_page(text)
            for url in urls:
                text = self.get_detail_page(url)
                data = self.parse_detail_page(text)
                self.save_to_mongo(data)







spider = Doubanmovies()

spider.start()


