# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/9/5

import csv
import json
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup


class BaseBilibili:
    def __init__(self):
        self.name = 'Base Bilibili Crawler'
        self.headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'sec-fetch-dest': 'script',
            'sec-fetch-mode': 'no-cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'
        }

    def get_channel_id(self, channel_url):
        response = requests.get(channel_url, headers=self.headers)
        soup = BeautifulSoup(response.text, "html")
        channel_id = int(list(filter(None, soup.find('link', rel='alternate').attrs['href'].split('/')))[-1])
        return channel_id

    def get_channel(self, channel_url, page=1):
        url = "https://s.search.bilibili.com/cate/search"
        headers = self.headers.update({'referer': channel_url})
        params = {
            'main_ver': 'v3',
            'search_type': 'video',
            'view_type': 'hot_rank',
            'copy_right': -1,
            'new_web_tag': 1,
            'order': 'click',
            'cate_id': self.get_channel_id(channel_url),
            'page': page,
            'pagesize': 30,
            'time_from': int(time.strftime("%Y%m%d", time.localtime())) - 7,
            'time_to': int(time.strftime("%Y%m%d", time.localtime()))
        }
        response = requests.get(url, headers=headers, params=params)
        response.encoding = 'utf-8'
        channel_json = json.loads(response.text)
        return channel_json

    def get_video(self, bvid):
        video_url = f'https://www.bilibili.com/video/{bvid}'
        response = requests.get(video_url, headers=self.headers)
        raw_data = response.text

        # video information
        video_text = raw_data[raw_data.index("\"videoData\":") + 12: raw_data.index(",\"upData\"")]
        video_json = json.loads(video_text)

        # uploader information
        author_text = raw_data[raw_data.index(",\"upData\":") + 10: raw_data.index(",\"isCollection\"")]
        author_json = json.loads(author_text)

        return video_json, author_json

    # def get_response(self, base_url: str, params: dict):
    #     response = requests.get(base_url, headers=self.headers, params=params)
    #     response.encoding = 'utf-8'
    #     return response

    # def cal_comment_page(self):
    #     comment_response = self.get_response(1)
    #
    #     crawler = CommentCrawler(bv)
    #     counts = crawler.get_comment(1)['data']['cursor']
    #     if 'all_count' in counts.keys():
    #         comment_counts = counts['all_count']
    #     pass

    @staticmethod
    def save_data(result, output_path: str = None, csv_header: list = None):
        if output_path:
            with open(output_path, 'a', newline='', encoding='utf-8-sig') as fp:
                csv_writer = csv.DictWriter(fp, csv_header)
                if fp.tell() == 0:
                    csv_writer.writeheader()
                csv_writer.writerows(result)


class BilibiliParser(BaseBilibili):
    def __init__(self):
        super().__init__()

    @staticmethod
    def parser_channel(channel_json):
        channel_result = channel_json['result']
        videos = []
        for video in channel_result:
            videos.append({'bvid': video['bvid'],
                           'url': video['arcurl'],
                           'tags': video['tag'],
                           'crawl_channel_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())})
        csv_header = ['bvid', 'url', 'tags', 'crawl_channel_time']
        return videos, csv_header

    @staticmethod
    def parser_video(video_json, author_json):
        video_detail = [{
            # video info
            'bvid': video_json['bvid'],  # 视频 bv 号
            'avid': video_json['aid'],  # 视频 av 号
            'cid': video_json['cid'],  # 视频 cid 号，用于爬取弹幕
            'title': video_json['title'],  # 视频标题
            'pubdate': datetime.fromtimestamp(video_json['pubdate']).strftime("%Y-%m-%d %H:%M:%S"),  # 发布日期
            'duration': video_json['duration'],  # 视频时长，单位秒
            'views': video_json['stat']['view'],  # 观看数
            'likes': video_json['stat']['like'],  # 点赞数
            'coins': video_json['stat']['coin'],  # 投币数
            'shares': video_json['stat']['share'],  # 转发数
            'favorites': video_json['stat']['favorite'],  # 收藏数
            'bullets': video_json['stat']['danmaku'],  # 弹幕数
            'comments': video_json['stat']['reply'],  # 评论数

            # uploader info
            'up_id': author_json['mid'],  # uploader user id
            'up_name': author_json['name'],  # uploader name
            'up_gender': author_json['sex'],  # uploader gender
            'up_fans': author_json['fans'],  # 作者粉丝数
            'up_following': author_json['attention'],  # 作者关注数
            'up_level': author_json['level_info']['current_level'],  # 作者等级
            'up_vip': author_json['vip']['label']['text'],  # 作者会员
            'up_official': author_json['Official']['title'],  # 作者认证
            'up_archives': author_json['archiveCount'],  # 作者共发布的视频数量

            'crawl_video_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }]

        csv_header = ['avid', 'bvid', 'cid', 'title', 'pubdate', 'duration',
                      'views', 'likes', 'coins', 'shares', 'favorites', 'bullets', 'comments',
                      'up_id', 'up_name', 'up_gender', 'up_fans', 'up_following',
                      'up_level', 'up_vip', 'up_official', 'up_archives', 'crawl_video_time']
        return video_detail, csv_header


    #
    # @staticmethod
    # def parser_video_detail(bvid):
    #     url = f'https://www.bilibili.com/video/{bvid}'  # TODO need bvid
    #     headers = {
    #         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    #                       '(KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
    #     }
    #     response = requests.get(url, headers)
    #     raw_data = response.text
    #     # 视频详细信息
    #     video_detail = raw_data[raw_data.index("\"videoData\":") + 12: raw_data.index(",\"upData\"")]
    #     video_detail_json = json.loads(video_detail)
    #     # up主相关信息
    #     author_detail = raw_data[raw_data.index(",\"upData\":") + 10: raw_data.index(",\"isCollection\"")]
    #     author_detail_json = json.loads(author_detail)
    #
    #     return video_detail_json, author_detail_json
    #
    # @staticmethod
    # def parser_comment(comment_response):
    #     comment_json = json.loads(comment_response.text)
    #     comments = comment_json['data']['replies']
    #     comment_result = []
    #     for comment in comments:
    #         comment_data = {
    #             'bv': self.bvid,  # 视频bv号  # TODO need bvid
    #             'comment_id': comment['rpid'],  # 评论id
    #             'comment_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comment['ctime'])),  # 评论时间，由时间戳转换
    #             'comment_user_id': comment['member']['mid'],  # 评论用户id
    #             'comment_user_name': comment['member']['uname'],  # 评论用户名
    #             'comment_content': comment['content']['message'],  # 评论内容
    #             'comment_likes': comment['like'],  # 评论点赞数
    #             'crawler_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #         }
    #         comment_result.append(comment_data)
    #
    #     return comment_result
    #
    # def parser_bullet(self):
    #     pass


class BilibiliCrawler(BaseBilibili):
    def __init__(self):
        super().__init__()
        self.parser = BilibiliParser()

    def crawl_channel(self, channel_url, output_path: str = None):

        pages = self.get_channel(channel_url)['numPages']

        all_videos = []
        for page in range(1, pages):
            time.sleep(1)
            channel_json = self.get_channel(channel_url, page=page)
            videos, csv_header = self.parser.parser_channel(channel_json)
            all_videos.append(videos)

            if output_path:
                self.save_data(videos, output_path, csv_header)
                print(f'Successfully save data to {output_path} from channel: {channel_url}, page: {page}')
            else:
                print(f'Successfully get result from channel: {channel_url}, page: {page}')

        all_videos_df = pd.DataFrame(all_videos)
        return all_videos_df

    def crawl_video(self, bvid_list: list, output_path: str = None):

        video_details = []
        for bvid in bvid_list:
            video_json, author_json = self.get_video(bvid)
            video_detail, csv_header = self.parser.parser_video(video_json, author_json)
            video_details.append(video_detail)

            if output_path:
                self.save_data(video_detail, output_path, csv_header)
                print(f'Successfully save data to {output_path} from video: {bvid}')
            else:
                print(f'Successfully get result from video: {bvid}')

        video_details_df = pd.DataFrame(video_details)
        return video_details_df



    # def crawl_comment(self, avid, page, output_path):
    #     url = 'https://api.bilibili.com/x/v2/reply/main'
    #
    #     params = {
    #         # 'callback': 'jQuery17201888299578386794_' + str(round(time.time() * 1000)),
    #         # 'jsonp': 'jsonp',
    #         'next': page,  # 页码
    #         'type': 1,
    #         'oid': avid,  # 视频av号
    #         'mode': 3,  # 评论排序方式
    #         # 'plat': 1,
    #         # '_': str(round(time.time() * 1000))  # 当前时间戳
    #     }
    #
    #     response = requests.get(url, headers=self.headers, params=params)
    #     response.encoding = 'utf-8'
    #     data_json = json.loads(response.text)
    #     comment_list = data_json['data']['replies']
    #
    #     comments = []
    #     for i in range(len(comment_list)):
    #         comment = {
    #             'id': comment_list[i]['rpid'],
    #             'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comment_list[i]['ctime'])),
    #             'parent': comment_list[i]['parent'],
    #             'like': comment_list[i]['like'],
    #             'user_id': comment_list[i]['member']['mid'],
    #             'user_name': comment_list[i]['member']['uname'],
    #             'content': comment_list[i]['content']['message'],
    #             'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    #         }
    #         comments.append(comment)
    #
    #     with open(output_path, 'a', newline='', encoding='utf-8-sig') as fp:
    #         csv_header = ['id', 'time', 'parent', 'like', 'user_id', 'user_name', 'content', 'crawl_time']
    #         csv_writer = csv.DictWriter(fp, csv_header)
    #         if fp.tell() == 0:
    #             csv_writer.writeheader()
    #         csv_writer.writerows(comments)
    #
    #     print(f'Successfully get replies from video: {avid}, page: {page + 1}')
    #     time.sleep(2)


if __name__ == '__main__':

    # get a list of videos from a channel
    my_channel_url = 'https://www.bilibili.com/v/food/detective'
    my_channel_output = r'Bilibili/video_list_channel.csv'

    crawler = BilibiliCrawler()
    crawler.crawl_channel(my_channel_url, my_channel_output)

    a = crawler.get_channel(my_channel_url)

    # get video details
    my_bvid = pd.read_csv(my_channel_output)['bvid']
    my_video_output = r'Bilibili/video_details.csv'

    crawler = BilibiliCrawler()
    crawler.crawl_video(my_bvid[0:5], my_video_output)

    ######################
    # my_bvid = 'BV16X4y1g7wT'
    # my_avid = 715024588
    # my_page = 3
    # my_output_path = 'Bilibili/result_BilibiliReply.csv'
