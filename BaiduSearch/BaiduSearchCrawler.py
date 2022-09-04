# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/9/4

import requests

import csv
from bs4 import BeautifulSoup
import time
import itertools


class BaiduSearchCrawler:
    def __init__(self, cookie: str):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit'
                          '/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'Cookie': cookie
        }
        self.url = 'https://www.baidu.com/s'

    def search_news(self,
                    words: list,
                    pages: int,
                    output_path: str):

        for word, page in itertools.product(words, range(pages)):

            params = {
                'rtt': 1,
                'bsst': 1,
                'cl': 2,
                'tn': 'news',
                'rsv_dl': 'ns_pc',
                'word': word,
                'pn': page
            }  # TODO different params

            response = requests.get(self.url, headers=self.headers, params=params)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, "html.parser")

            items = soup.find("div", id="content_left").findAll("div", class_="result-op c-container xpath-log new-pmd")

            results = []
            for item in items:

                try:
                    date = item.find("div", class_="c-span-last").find("span", class_="c-color-gray2").text
                except AttributeError:
                    date = None

                result = {
                    'title': item.find("h3").find("a").text,
                    'abstract': item.find("div", class_="c-span-last").find("span", class_="c-color-text").text,
                    'url': item.find("h3").find("a")["href"],
                    'source': item.find("div", class_="c-span-last").find("span", class_="c-color-gray").text,
                    'date': date,
                    'search_word': word,
                    'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                }
                results.append(result)

            with open(output_path, 'a', newline='', encoding='utf-8-sig') as fp:
                csv_header = ['title', 'abstract', 'url', 'source', 'date', 'search_word', 'crawl_time']
                csv_writer = csv.DictWriter(fp, csv_header)
                if fp.tell() == 0:
                    csv_writer.writeheader()
                csv_writer.writerows(results)

            print(f'Successfully get result from word: {word}, page: {page + 1}')
            time.sleep(2)


if __name__ == '__main__':

    # settings start
    my_cookie = ''  # your cookie after login
    my_words = ['健康']  # a list of keywords used to search. e.g., ['健康']， ['健康', '养生']
    my_pages = 3  # to crawl how many pages
    my_output_path = r'BaiduSearch/result_BaiduNewsSearch.csv'  # path to save data
    # settings end

    crawler = BaiduSearchCrawler(cookie=my_cookie)
    crawler.search_news(words=my_words, pages=my_pages, output_path=my_output_path)
