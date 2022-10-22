# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/10/17

import csv
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


class BaseXimalayaFM:
    def __init__(self):
        self.name = 'XimalayaFM Base Crawler'
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'}
        self.base_url = "https://www.ximalaya.com/"

    def get_category(self, url):
        response = requests.get(url, headers=self.headers)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "html")
        cate_result = soup.find('div', class_='content').find_all("li")
        return cate_result

    def get_album(self, url):
        response = requests.get(url, headers=self.headers)
        response.encoding = 'utf-8'
        album_result = BeautifulSoup(response.text, "html")
        return album_result

    def _generate_cate_urls(self, category: str, page: int = None):
        """
        generate urls of all pages for a category
        :param category: str. name of the category
        :param page: int. how many pages you want to crawl. defaults to None, i.e., crawl all pages shown in the category (50 pages)
        :return: list. a list of urls
        """
        if not page or page > 50:
            print("NOTE: page should between 1 to 50, default crawl 50 pages")
            urls = [self.base_url + category + "/"]
            for p in range(2, 51):
                urls.append(self.base_url + category + f'/p{p}/')
        elif page == 1:
            urls = [self.base_url + category + "/"]
        else:
            urls = [self.base_url + category + "/"]
            for p in range(2, page + 1):
                urls.append(self.base_url + category + f'/p{p}/')
        return urls

    @staticmethod
    def save_data(result, output_path: str = None, csv_header: list = None):
        """
        to save crawl result in a csv file.
        :param result: a pandas.DataFrame result or a list
        :param output_path: path to save the csv file
        :param csv_header: headers of the csv
        :return: None. (a csv file saved in the output path)
        """
        if output_path:
            with open(output_path, 'a', newline='', encoding='utf-8-sig') as fp:
                csv_writer = csv.DictWriter(fp, csv_header)
                if fp.tell() == 0:
                    csv_writer.writeheader()
                csv_writer.writerows(result)

    @staticmethod
    def _format_listens(listens):

        if '万' in listens:
            listens_clean = int(listens.replace('万', '').replace('.', '')) * 10 ** 3
        elif '亿' in listens:
            listens_clean = int(listens.replace('亿', '').replace('.', '')) * 10 ** 7
        else:
            listens_clean = int(listens)

        return listens_clean


class XimalayaFMParser(BaseXimalayaFM):
    def __init__(self):
        super().__init__()
        self.name = 'XimalayaFM Parser'

    def parser_category(self, cate_result):
        cate_items = []
        for item in cate_result:

            paid_type = item.div.a.attrs['class'][1].replace('corner-', '').replace('-mark', '').replace('-', ' ')
            # four types. lg: free; vip: vip; paid: paid; limit free: limit free
            if paid_type == 'lg':
                paid_type = 'free'
            else:
                paid_type = paid_type

            cate_items.append({
                'title': item.find('a', class_='album-title line-1 lg bold T_G').attrs['title'],
                'author': item.find('a', class_='album-author T_G').get_text(),
                'url': item.find('a', class_='album-title line-1 lg bold T_G').attrs['href'],
                'paid_type': paid_type,
                'listens': self._format_listens(item.find('p', class_='listen-count _hW').get_text()),
                'cate_crawl_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            })
        csv_header = ['title', 'author', 'url', 'paid_type', 'listens', 'category', 'cate_crawl_time']
        return cate_items, csv_header

    def parser_album(self, album_result):

        if album_result.find('span', class_='d-ib v-m dC_'):
            score = float(album_result.find('span', class_='d-ib v-m dC_').get_text().replace('分', ''))
        else:
            score = '暂无评分'

        if album_result.find('article', class_='intro Q_v'):
            intro = album_result.find('article', class_='intro Q_v').get_text()
        else:
            intro = '暂无简介'

        details = [{
            'title': album_result.find('h1', class_='title dC_').get_text(),
            'score': score,
            'listens': self._format_listens(album_result.find('span', class_='count dC_').get_text()),
            'tags': [tag.text for tag in album_result.find('div', class_='album-tags tags _If').contents],
            'intro': intro,
            'voices': int(album_result.find('span', class_='title active s_O').get_text().replace('声音（', '').replace('）', '')),
            'comments': album_result.find('span', class_='title false s_O').get_text().replace('评价（', '').replace('）', ''),
            # comments may be "999+"
            'album_crawl_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }]

        csv_header = ['url', 'title', 'score', 'listens', 'tags', 'intro', 'voices', 'comments', 'album_crawl_time']
        return details, csv_header


class XimalayaFMCrawler(BaseXimalayaFM):
    def __init__(self):
        super().__init__()
        self.name = 'XimalayaFM Crawler'
        self.parser = XimalayaFMParser()

    def crawl_category(self, category: str = 'shangye', output_path: str = None, page: int = None):
        urls = self._generate_cate_urls(category, page)

        all_items = []
        for u in urls:
            cate_result = self.get_category(u)
            cate_items, csv_header = self.parser.parser_category(cate_result)
            cate_items = [dict(item, **{'category': category}) for item in cate_items]
            all_items.extend(cate_items)

            if output_path:
                self.save_data(cate_items, output_path, csv_header)
                print(f'Successfully save data to {output_path} from category: {category}, url: {u}')
            else:
                print(f'Successfully get result from from category: {category}, url: {u}')

        all_items_df = pd.DataFrame(all_items)
        return all_items_df

    def crawl_album(self, urls: list, output_path: str = None):

        albums = []
        for u in urls:
            album_result = self.get_album(u)
            album_details, csv_header = self.parser.parser_album(album_result)
            album_details = [dict(item, **{'url': u}) for item in album_details]
            albums.extend(album_details)

            if output_path:
                self.save_data(album_details, output_path, csv_header)
                print(f'Successfully save data to {output_path} from album: {u}')
            else:
                print(f'Successfully get result from from album: {u}')

        albums_df = pd.DataFrame(albums)
        return albums_df


if __name__ == '__main__':

    # crawler basic album information from a category
    crawler = XimalayaFMCrawler()
    my_category = 'shangye'
    my_cate_output = r'XimalayaFM/result_XimalayaFM_shangye.csv'
    category_results = crawler.crawl_category(my_category, my_cate_output, page=3)  # 1 <= page <= 50, default 50 pages

    # album
    my_album_output = r'XimalayaFM/result_XimalayaFM_shangye_albums.csv'
    data_category = pd.read_csv(my_cate_output)
    album_results = crawler.crawl_album(data_category['url'], my_album_output)
