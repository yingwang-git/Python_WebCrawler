# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/9/8

import csv
import itertools
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


class BaseBaidu:

    def __init__(self):
        self.name = 'Baidu Base Crawler'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit'
                          '/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }

    def _get_response(self, base_url: str, params: dict):
        response = requests.get(base_url, headers=self.headers, params=params)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "html.parser")
        return soup

    @staticmethod
    def _save_data(result, output_path: str = None, csv_header: list = None):
        if output_path:
            with open(output_path, 'a', newline='', encoding='utf-8-sig') as fp:
                csv_writer = csv.DictWriter(fp, csv_header)
                if fp.tell() == 0:
                    csv_writer.writeheader()
                csv_writer.writerows(result)


class BaiduParser(BaseBaidu):

    def __init__(self):
        super().__init__()
        self.name = 'Baidu Parser'

    @staticmethod
    def parse_news(soup: BeautifulSoup):

        news_items = soup.find("div", id="content_left").findAll("div", class_="result-op c-container xpath-log new-pmd")

        news_results = []
        for item in news_items:

            try:
                date = item.find("div", class_="c-span-last").find("span", class_="c-color-gray2").text
            except AttributeError:
                date = None

            news_result = {
                'title': item.find("h3").find("a").text,
                'abstract': item.find("div", class_="c-span-last").find("span", class_="c-color-text").text,
                'url': item.find("h3").find("a")["href"],
                'source': item.find("div", class_="c-span-last").find("span", class_="c-color-gray").text,
                'date': date,
                'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            }
            news_results.append(news_result)

        csv_header = ['title', 'abstract', 'url', 'source', 'date', 'crawl_time', 'search_word']

        return news_results, csv_header


class BaiduCrawler(BaseBaidu):

    def __init__(self, cookie):
        super().__init__()
        self.name = 'Baidu Crawler'
        self.headers['Cookie'] = cookie
        self.parser = BaiduParser()

    def search_news(self, words: list, pages: int, output_path: str = None):

        """
        :param words: list. a list of words used to search.
        :param pages: int. how many pages you want to crawl.
        :param output_path: str or None. Path used to save data. Defaults to None.
        :return: a pandas.DataFrame of results.
        """
        base_url = 'https://www.baidu.com/s'

        all_news = []
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

            news_soup = self._get_response(base_url=base_url, params=params)
            news_results, csv_header = self.parser.parse_news(news_soup)
            news_results = [dict(item, **{'search_word': word}) for item in news_results]
            all_news.extend(news_results)

            if output_path:
                self._save_data(news_results, output_path=output_path, csv_header=csv_header)
                print(f'Successfully save result to {output_path} from word: {word}, page: {page + 1}')
            else:
                print(f'Successfully get result from word: {word}, page: {page + 1}')

        all_news_df = pd.DataFrame(all_news)
        return all_news_df


if __name__ == '__main__':

    # settings start
    my_cookie = ''  # your cookie after login
    my_words = ['健康']  # a list of keywords used to search. e.g., ['健康']， ['健康', '养生']
    my_pages = 3  # to crawl how many pages
    my_output_path = r'BaiduSearch/result_BaiduNewsSearch.csv'  # path to save data
    # settings end

    crawler = BaiduCrawler(cookie=my_cookie)
    my_results = crawler.search_news(words=my_words, pages=my_pages, output_path=my_output_path)