# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/9/5

import csv
import json
import time

import requests


class BilibiliCrawler:
    def __init__(self, bvid):
        self.headers = {
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'referer': f'https://www.bilibili.com/video/{bvid}',
                'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': 'Windows',
                'sec-fetch-dest': 'script',
                'sec-fetch-mode': 'no-cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'
        }

    def crawl_reply(self, avid, page, output_path):
        url = 'https://api.bilibili.com/x/v2/reply/main'

        params = {
            # 'callback': 'jQuery17201888299578386794_' + str(round(time.time() * 1000)),
            # 'jsonp': 'jsonp',
            'next': page,  # 页码
            'type': 1,
            'oid': avid,  # 视频av号
            'mode': 3,  # 评论排序方式
            # 'plat': 1,
            # '_': str(round(time.time() * 1000))  # 当前时间戳
        }

        response = requests.get(url, headers=self.headers, params=params)
        response.encoding = 'utf-8'
        data_json = json.loads(response.text)
        comment_list = data_json['data']['replies']

        comments = []
        for i in range(len(comment_list)):
            comment = {
                'id': comment_list[i]['rpid'],
                'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comment_list[i]['ctime'])),
                'parent': comment_list[i]['parent'],
                'like': comment_list[i]['like'],
                'user_id': comment_list[i]['member']['mid'],
                'user_name': comment_list[i]['member']['uname'],
                'content': comment_list[i]['content']['message'],
                'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            }
            comments.append(comment)

        with open(output_path, 'a', newline='', encoding='utf-8-sig') as fp:
            csv_header = ['id', 'time', 'parent', 'like', 'user_id', 'user_name', 'content', 'crawl_time']
            csv_writer = csv.DictWriter(fp, csv_header)
            if fp.tell() == 0:
                csv_writer.writeheader()
            csv_writer.writerows(comments)

        print(f'Successfully get replies from video: {avid}, page: {page + 1}')
        time.sleep(2)


if __name__ == '__main__':

    my_bvid = 'BV16X4y1g7wT'
    my_avid = 715024588
    my_page = 3
    my_output_path = 'Bilibili/result_BilibiliReply.csv'

    crawler = BilibiliCrawler(bvid=my_bvid)
    for p in range(my_page):
        crawler.crawl_reply(avid=my_avid, page=p, output_path=my_output_path)

    # TODO total_counts = data_json['data']['cursor']['all_count']
    # TODO conversion between bvid and avid
