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
        self.name = 'Bilibili Base Crawler'
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

    def get_channel(self, channel_url: str, page: int = 1):
        """
        get responses from a bilibili channel. to crawl all videos upload in recent seven days in a channel
        :param channel_url: the url of the channel
        :param page: page
        :return: a json result of video basic information from a channel
        """
        url = "https://s.search.bilibili.com/cate/search"
        headers = self.headers.update({'referer': channel_url})
        params = {
            'main_ver': 'v3',
            'search_type': 'video',
            'view_type': 'hot_rank',
            'copy_right': -1,
            'new_web_tag': 1,
            'order': 'click',
            'cate_id': self._get_channel_id(channel_url),
            'page': page,
            'pagesize': 30,
            'time_from': int(time.strftime("%Y%m%d", time.localtime())) - 7,
            'time_to': int(time.strftime("%Y%m%d", time.localtime()))
        }
        response = requests.get(url, headers=headers, params=params)
        response.encoding = 'utf-8'
        channel_json = json.loads(response.text)
        return channel_json

    def get_video(self, bvid: str):
        """
        get responses from a video page
        :param bvid: the bvid of the video
        :return: a json result of video details, and a json result of uploader information
        """
        video_url = f'https://www.bilibili.com/video/{bvid}'
        response = requests.get(video_url, headers=self.headers)
        raw_data = response.text

        # video information
        video_text = raw_data[raw_data.index("\"videoData\":") + 12: raw_data.index(",\"upData\"")]
        video_json = json.loads(video_text)

        # uploader information
        uploader_text = raw_data[raw_data.index(",\"upData\":") + 10: raw_data.index(",\"isCollection\"")]
        uploader_json = json.loads(uploader_text)

        return video_json, uploader_json

    def get_comment(self, bvid: str, page: int = 0):
        """
        get responses from video comments
        :param bvid: the bvid of the video
        :param page: page
        :return: a json result of all comments from the video
        """
        url = 'https://api.bilibili.com/x/v2/reply/main'
        headers = self.headers.update({'referer': f'https://www.bilibili.com/video/{bvid}'})
        avid = self._get_avid(bvid)

        params = {
            'next': page,
            'type': 1,
            'oid': avid
        }

        response = requests.get(url, headers=headers, params=params)
        response.encoding = 'utf-8'
        comment_json = json.loads(response.text)

        return comment_json

    def get_bullet(self, bvid: str):
        """
        get responses from video bullets
        :param bvid: the bvid of the video
        :return: a BeautifulSoup result of all bullet comments from the video
        """
        cid = self._get_cid(bvid)
        url = f'https://comment.bilibili.com/{cid}.xml'
        response = requests.get(url, headers=self.headers)
        response.encoding = 'utf-8'
        bullet_soup = BeautifulSoup(response.text, "xml")
        bullet_result = bullet_soup.find_all("d")
        return bullet_result

    def _get_channel_id(self, channel_url: str):
        """
        get the channel id from a channel. for crawling channel videos
        :param channel_url: the url from a channel
        :return: the channel id
        """
        headers = self.headers.update({'referer': channel_url})
        response = requests.get(channel_url, headers=headers)
        soup = BeautifulSoup(response.text, "html")
        channel_id = int(list(filter(None, soup.find('link', rel='alternate').attrs['href'].split('/')))[-1])
        return channel_id

    def _get_avid(self, bvid: str):
        """
        get the avid from a video. for crawling video comments.
        :param bvid: the bvid of the video
        :return: the avid of the video
        """
        video_json, _ = self.get_video(bvid)
        return video_json['aid']

    def _get_cid(self, bvid: str):
        """
        get the cid from a video. for crawling video bullet comments.
        :param bvid: the bvid of the video
        :return: the cid of the video
        """
        video_json, _ = self.get_video(bvid)
        return video_json['cid']

    @staticmethod
    def save_data(result, output_path: str = None, csv_header: list = None):
        """
        to save crawl result in a csv file.
        :param result: a pandas.DataFrame result
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


class BilibiliParser(BaseBilibili):
    def __init__(self):
        super().__init__()
        self.name = "Bilibili Parser"

    @staticmethod
    def parser_channel(channel_json):
        """
        parse channel result
        :param channel_json: a json result from get_channel()
        :return: a list of structured results extracted from the json file, and a list of names as the csv header
        """
        channel_result = channel_json['result']
        videos = []
        for video in channel_result:
            videos.append({'bvid': video['bvid'],  # bvid
                           'url': video['arcurl'],  # video url
                           'tags': video['tag'],  # video tags
                           'channel_crawl_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())})
        csv_header = ['bvid', 'url', 'tags', 'channel_crawl_time']
        return videos, csv_header

    @staticmethod
    def parser_video(video_json, uploader_json):
        """
        parse video details and uploader information
        :param video_json: a json result from get_video()
        :param uploader_json: a json result from get_video()
        :return: a list of structured results extracted from the json files, and a list of names as the csv header
        """
        video_detail = [{
            # video info
            'bvid': video_json['bvid'],  # bvid
            'avid': video_json['aid'],  # avid, for crawling comments
            'cid': video_json['cid'],  # cid, for crawling bullets
            'title': video_json['title'],   # video title
            'pubdate': datetime.fromtimestamp(video_json['pubdate']).strftime("%Y-%m-%d %H:%M:%S"),  # upload datetime
            'duration': video_json['duration'],  # duration of the video, in seconds
            'views': video_json['stat']['view'],  # number of views
            'likes': video_json['stat']['like'],  # number of likes
            'coins': video_json['stat']['coin'],  # number of coins
            'shares': video_json['stat']['share'],  # number of shares
            'favorites': video_json['stat']['favorite'],  # number of favorites
            'bullets': video_json['stat']['danmaku'],  # number of bullets
            'comments': video_json['stat']['reply'],  # number of comments

            # uploader info
            'up_id': uploader_json['mid'],  # uploader user id
            'up_name': uploader_json['name'],  # uploader name
            'up_gender': uploader_json['sex'],  # uploader gender
            'up_fans': uploader_json['fans'],  # number of fans
            'up_following': uploader_json['attention'],  # number of following
            'up_level': uploader_json['level_info']['current_level'],  # level of the uploader
            'up_vip': uploader_json['vip']['label']['text'],  # vip info of the uploader
            'up_official': uploader_json['Official']['title'],  # certification info of the uploader
            'up_archives': uploader_json['archiveCount'],  # total number of videos uploaded by the uploader

            'video_crawl_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }]

        csv_header = ['avid', 'bvid', 'cid', 'title', 'pubdate', 'duration',
                      'views', 'likes', 'coins', 'shares', 'favorites', 'bullets', 'comments',
                      'up_id', 'up_name', 'up_gender', 'up_fans', 'up_following',
                      'up_level', 'up_vip', 'up_official', 'up_archives', 'video_crawl_time']
        return video_detail, csv_header

    @staticmethod
    def parser_comment(comment_result):
        """
        parse comments
        :param comment_result: result extracted from comment_json['data']['replies']
        :return: a list of structured results extracted from the json file, and a list of names as the csv header
        """
        comments = []
        for comment in comment_result:
            comment_detail = {
                'comment_id': comment['rpid'],  # comment id
                'comment_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(comment['ctime'])),  # time post the comment
                'comment_user_id': comment['member']['mid'],   # user id
                'comment_user_name': comment['member']['uname'],  # user name
                'comment_content': comment['content']['message'],  # comment content
                'comment_likes': comment['like'],   # number of likes of the comment
                'comment_crawler_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            comments.append(comment_detail)

        csv_header = ['comment_id', 'comment_time', 'comment_user_id', 'comment_user_name',
                      'comment_content', 'comment_likes', 'comment_crawler_time']

        return comments, csv_header

    @staticmethod
    def parser_bullet(bullet_result):
        """
        parse bullets
        :param bullet_result: bullet results from get_bullet()
        :return: a list of structured results extracted from the json file, and a list of names as the csv header
        """
        bullets = []
        for bullet in bullet_result:
            bullet_detail = {
                'bullet_content': bullet.text,  # bullet content
                'bullet_entry': str.split(bullet.attrs['p'], ',')[0],  # the seconds when the bullet enter the video
                'bullet_time': time.strftime("%Y-%m-%d %H:%M:%S",
                                             time.localtime(int(str.split(bullet.attrs['p'], ',')[4]))),  # bullet post time
                'bullet_crawler_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }
            bullets.append(bullet_detail)

        csv_header = ['bullet_content', 'bullet_entry', 'bullet_time', 'bullet_crawler_time']

        return bullets, csv_header


class BilibiliCrawler(BaseBilibili):
    def __init__(self):
        super().__init__()
        self.name = 'Bilibili Crawler'
        self.parser = BilibiliParser()

    def crawl_channel(self, channel_url, output_path: str = None):
        """
        crawl all videos from a bilibili channel (uploaded in recent seven days)
        :param channel_url: the url of the channel
        :param output_path: path to save the result, default to None
        :return: if no output path, return a pandas.DataFrame; else, also return a csv file saved in the output path
        """
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
        """
        crawl video details and uploader information from video(s)
        :param bvid_list: a list of bvid of video(s). Note that one bvid should also be in a list.
        :param output_path: path to save the result, default to None
        :return: if no output path, return a pandas.DataFrame; else, also return a csv file saved in the output path
        """
        video_details = []
        for bvid in bvid_list:
            time.sleep(0.5)
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

    def crawl_comment(self, bvid_list: list, output_path: str = None):
        """
        crawl all comments from video(s)
        :param bvid_list: a list of bvid of video(s). Note that one bvid should also be in a list.
        :param output_path: path to save the result, default to None
        :return: if no output path, return a pandas.DataFrame; else, also return a csv file saved in the output path
        """

        comment_all = []
        for bvid in bvid_list:
            counts = self.get_comment(bvid)['data']['cursor']

            if 'all_count' in counts.keys():
                pages = counts['all_count'] // 20 + 1
                print(f"A total of {pages} pages of comments in video {bvid}")  # the pages may be larger than actual pages

                for page in range(pages):
                    time.sleep(0.5)
                    comment_json = self.get_comment(bvid, page)

                    comment_result = comment_json['data']['replies']
                    if comment_result:
                        comments, csv_header = self.parser.parser_comment(comment_result)
                        comments = [dict(**{'bvid': bvid}, **item) for item in comments]
                        csv_header.insert(0, 'bvid')
                        comment_all.append(comments)
                        if output_path:
                            self.save_data(comments, output_path, csv_header)
                            print(f'Successfully save comment page {page + 1} to {output_path} from video: {bvid}')
                        else:
                            print(f'Successfully get comment page {page + 1} from video: {bvid}')
                    else:
                        print(f'all comments were crawled from video {bvid}')
                        break

            else:
                print(f'There is no comment of video {bvid}')
                pass

        comment_all_df = pd.DataFrame(comment_all)
        return comment_all_df

    def crawl_bullet(self, bvid_list: list, output_path: str = None):
        """
        crawl all bullets from video(s)
        :param bvid_list: a list of bvid of video(s). Note that one bvid should also be in a list.
        :param output_path: path to save the result, default to None
        :return: if no output path, return a pandas.DataFrame; else, also return a csv file saved in the output path
        """

        bullet_all = []
        for bvid in bvid_list:
            time.sleep(1)
            bullet_result = self.get_bullet(bvid)
            if len(bullet_result) != 0:
                print(f'A total of {len(bullet_result)} bullets in video {bvid}')
                bullets, csv_header = self.parser.parser_bullet(bullet_result)
                bullets = [dict(**{'bvid': bvid}, **item) for item in bullets]
                csv_header.insert(0, 'bvid')
                bullet_all.append(bullets)
                if output_path:
                    self.save_data(bullets, output_path, csv_header)
                    print(f'Successfully save bullets to {output_path} from video: {bvid}')
                else:
                    print(f'Successfully get bullets from video: {bvid}')
            else:
                print(f'There is no bullet of video {bvid}')
                pass

        bullet_all_df = pd.DataFrame(bullet_all)
        return bullet_all_df


if __name__ == '__main__':

    # get a list of videos from a channel
    my_channel_url = 'https://www.bilibili.com/v/knowledge/science'
    my_channel_output = r'Bilibili/result_BilibiliChannelVideos.csv'
    crawler = BilibiliCrawler()
    channel_results = crawler.crawl_channel(my_channel_url, my_channel_output)

    # get video details
    my_bvid = pd.read_csv(my_channel_output)['bvid']
    # my_bvid = ['BV16X4y1g7wT']  # one video bvid should also in a list
    my_video_output = r'Bilibili/result_BilibiliVideoDetails.csv'
    video_results = crawler.crawl_video(my_bvid[0:10], my_video_output)

    # get comment from video(s)
    my_comment_output = 'Bilibili/result_BilibiliComments.csv'
    comment_results = crawler.crawl_comment(my_bvid[0:1], my_comment_output)

    # get bullet from video(s)
    my_bullet_output = 'Bilibili/result_BilibiliBullets.csv'
    bullet_results = crawler.crawl_bullet(my_bvid[0:2], my_bullet_output)

