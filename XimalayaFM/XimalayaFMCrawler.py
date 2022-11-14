# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/11/7

import csv
import json
import time

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor


class BaseXimalayaFM:
    def __init__(self, output_dir, download_dir):
        self.name = 'XimalayaFM Base Crawler'
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'}
        self.output_dir = output_dir  # directory saving csv results
        self.download_dir = download_dir  # directory saving downloaded audios

    def get_category(self, category: str, subcategory: str = None,
                     page: int = 1, filters: dict = None):
        cate_url = 'https://www.ximalaya.com/revision/category/queryCategoryPageAlbums'
        category_code, subcategory_code = self._format_category(category, subcategory)
        params = {
            'category': category_code,
            'subcategory': subcategory_code,
            'sort': 0,
            'page': page,
            'perPage': 50,
            'useCache': 'false'
        }
        if category_code == 'youshengshu' and filters:
            params['meta'] = '-'.join([crawler._format_filters(k, v) for k, v in filters.items()])
        cate_response = requests.get(cate_url, headers=self.headers, params=params)
        return json.loads(cate_response.text)

    def get_json(self, url: str):
        response = requests.get(url, headers=self.headers)
        return json.loads(response.text)

    def save_csv(self, result: list, output_file: str):
        with open(f'{self.output_dir}/{output_file}', 'a', newline='', encoding='utf-8-sig') as fp:
            csv_writer = csv.DictWriter(fp, list(result[0].keys()))
            if fp.tell() == 0:
                csv_writer.writeheader()
            csv_writer.writerows(result)

    def _get_categories_map(self):
        categories_url = 'https://m.ximalaya.com/m-revision/page/category/queryCategories'
        categories = self.get_json(categories_url)['data']
        category_list = []
        for item in categories:
            category_details = {
                'category_id': item['category']['categoryId'],
                'category_name': item['category']['displayName'],
                'category_code': item['category']['code'],
                'category_link': item['category']['link']
            }
            for subcategory in item['category']['subCategories']:
                subcategory_details = {
                    'subcategory_id': subcategory['subCategoryId'],
                    'subcategory_name': subcategory['displayValue'],
                    'subcategory_code': subcategory['code'],
                    'subcategory_link': subcategory['link']
                }
                category_list.append({**category_details, **subcategory_details})
        # pd.DataFrame(category_list).to_csv(r'XimalayaFM/dict_categories.csv', index=False, encoding='utf-8-sig')
        return pd.DataFrame(category_list)

    def _get_category_page(self, category: str, subcategory: str = None, filters: str = None):
        total = self.get_category(page=1, category=category, subcategory=subcategory,
                                  filters=filters)['data']['total']
        return total // 50 + 1

    def _format_category(self, category: str, subcategory: str = None):
        categories = self._get_categories_map()
        # categories = pd.read_csv(r'XimalayaFM/dict_categories.csv')
        cate = categories[['category_name', 'category_code']]
        if category.lower() not in cate.values:
            print(f'ERROR: category {category} not exist! Please check!')
            return False
        category_code = (category.lower() if cate.columns[cate.eq(category.lower()).any()][0] == 'category_code'
                         else cate.loc[cate['category_name'] == category, 'category_code'].iloc[0])
        if not subcategory:
            return category_code, ''
        _ = categories.loc[categories['category_code'] == category_code].reset_index(drop=True)
        subcate = _[['subcategory_name', 'subcategory_code']]
        if subcategory.lower() not in subcate.values:
            print(f'ERROR: subcategory {subcategory} not exist in category {category}! Please check!')
            return False
        subcate_code = (
            subcategory.lower() if subcate.columns[subcate.eq(subcategory.lower()).any()][0] == 'subcategory_code'
            else subcate.loc[subcate['subcategory_name'] == subcategory, 'subcategory_code'].iloc[0])
        return category_code, subcate_code

    @staticmethod
    def _format_filters(key, value):
        filter_map = {
            'announcer': {'single': '272_4361', 'double': '272_4362', 'multiple': '272_4363'},  # number of announcer
            'finished': {'no': '131_2559', 'yes': '131_2560'},  # is finished
            'paid': {'no': '132_2722', 'yes': '132_2721'}  # is paid
        }
        if not filter_map.get(key.lower()):
            print("ERROR: invalid filter! 'announcer' should be 'single', 'double',or 'multiple'; "
                  "'finished' should be 'no' or 'yes'; 'paid' should be 'no' or 'yes' ")
            return False
        return filter_map[key.lower()][value]


class XimalayaFMParser(BaseXimalayaFM):
    def __init__(self, output_dir, download_dir):
        super().__init__(output_dir, download_dir)
        self.name = 'XimalayaFM Parser'

    @staticmethod
    def parse_category(category_json):
        albums = []
        for album in category_json['data']['albums']:
            albums.append({
                'album_id': album['albumId'],
                'album_paid': album['isPaid'],  # 是否付费。True=需会员或付费，False=免费
                'album_finished': album['isFinished'],  # 是否完结。0=无此属性，1=连载，2=完结
                'album_vipType': album['vipType'],  # 付费类型。0=仅付费，1=仅会员，2=会员或付费
            })
        return albums

    def parse_album(self, album_id):
        album_url = f'https://mobile.ximalaya.com/mobile/v1/album/ts-{round(time.time() * 1000)}?albumId={album_id}'
        album = self.get_json(album_url)['data']['album']
        details = [{
            **{'album_id': album_id,
               'album_title': album['title'],
               'album_subtitle': album['customSubTitle'],
               'album_info': album['intro'],
               'album_tags': album.get('tags'),
               'album_cover': album['coverSmall'].split('!')[0],
               'album_score': album.get('score'),  # 免费专辑无此属性
               'album_score_10': self._get_album_score(album['albumId']),
               'album_create': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(album['createdAt'] / 1000)),
               'album_tracks': album['tracks'],
               'album_plays': album['playTimes'],
               'album_comments': album.get('commentsCount'),
               'album_subscribes': album['subscribeCount']},
            **self._parse_album_price(album['albumId']),
            **self._parse_author(album['uid']),
            **self._parse_author_verify(album['uid'])
        }]
        return details

    def parse_track(self, album_id):
        album_url = f'https://mobile.ximalaya.com/mobile/v1/album/track/ts-{round(time.time() * 1000)}?' \
                    f'albumId={album_id}&pageSize=50&pageId='
        track_json = self.get_json(album_url + '1')['data']

        track_details = []
        for page in range(1, track_json['maxPageId'] + 1):
            tracks = self.get_json(album_url + str(page))['data']['list']
            for track in tracks:
                if not track['playUrl32']:
                    pass
                track_detail = {
                    'album_id': album_id,
                    'track_id': track['trackId'],
                    'track_name': track['title'],
                    'track_duration': track['duration'],
                    'track_plays': track['playtimes'],
                    'track_likes': track['likes'],
                    'track_comments': track['comments'],
                    'track_create': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(track['createdAt'] / 1000)),
                    'track_audio': track['playUrl32']
                }
                track_details.append(track_detail)
        return track_details

    def _get_album_score(self, album_id):
        score_url = f'https://www.ximalaya.com/revision/comment/albumStatistics/{album_id}'
        return self.get_json(score_url)['data'].get('albumScore')  # 10分制得分，页面显示的得分

    def _parse_album_price(self, album_id):
        price_url = f'https://www.ximalaya.com/revision/bdsp/album/pay/schema?id={album_id}&productType=1'
        price = self.get_json(price_url)['data']['albumPrice']

        if price.get('retailAlbum'):
            album_price = price['retailAlbum']['unBroughtTotalAmount']
        elif price.get('wholeAlbum'):
            album_price = price['wholeAlbum']['price']
        else:
            album_price = None

        return {
            'album_paid_type': price['paidType'],  # 价格类型。0=vip专享或者免费（无价格），1=整张购买，2=分集购买
            'album_price': album_price,
            'album_price_single': (None if not price.get('retailAlbum') else price['retailAlbum']['singlePrice'])
        }

    def _parse_author(self, author_id):
        author_url = f'https://www.ximalaya.com/revision/user/basic?uid={author_id}'
        author = self.get_json(author_url)['data']
        return {
            'author_id': author['uid'],
            'author_name': author['nickName'],
            'author_gender': author['gender'],
            'author_level': author['anchorGrade'],
            'author_vip': author['isVip'],
            'author_signature': author.get('personalSignature'),
            'author_desc': author.get('personalDescription'),
            'author_headimg': 'https' + author['cover'],
            'author_following': author['followingCount'],
            'author_followers': author['fansCount'],
            'author_albums': author['albumsCount'],
            'author_tracks': author['tracksCount']
        }

    def _parse_author_verify(self, author_id):
        verify_url = f'https://m.ximalaya.com/m-revision/page/anchor/queryAnchorPage/{author_id}'
        verify = self.get_json(verify_url)['data']['anchorInfo']['userInfo']
        return {
            'author_verified': verify['verifyStatus'],
            'author_verified_type': verify['verifyType'],
            'author_verified_desc': verify.get('ptitle')
        }


class XimalayaFMCrawler(BaseXimalayaFM):
    def __init__(self, output_dir, download_dir):
        super().__init__(output_dir, download_dir)
        self.name = 'XimalayaFM Crawler'
        self.parser = XimalayaFMParser(output_dir, download_dir)

    def crawler_category(self, output_file: str, category: str, subcategories: list = None,
                         filters: dict = None, pages: int = None, threads: int = 10):
        with ThreadPoolExecutor(threads) as t:
            for subcategory in subcategories:
                total_pages = self._get_category_page(category=category, subcategory=subcategory, filters=filters)
                for page in range(1, pages + 1 if pages and pages <= total_pages else total_pages + 1):
                    t.submit(self._crawl_category, category=category, subcategory=subcategory,
                             filters=filters, page=page, output_file=output_file)

    def _crawl_category(self, output_file: str, category: str, subcategory: str = None,
                        filters: dict = None, page: int = None):
        albums = self.get_category(category=category, subcategory=subcategory,
                                   filters=filters, page=page)
        albums_result = [{**album, **{'subcategory': subcategory}}
                         for album in self.parser.parse_category(albums)]
        self.save_csv(albums_result, output_file=output_file)
        print(f'Successfully save result to {output_file} from category: {category}, {subcategory}, page: {page}')

    def crawler_album(self, album_id_list: list, output_file: str, threads: int = 10):
        with ThreadPoolExecutor(threads) as t:
            for album_id in album_id_list:
                t.submit(self._crawl_album, album_id=album_id, output_file=output_file)

    def _crawl_album(self, album_id, output_file: str):
        album_details = self.parser.parse_album(album_id)
        self.save_csv(album_details, output_file=output_file)
        print(f'Successfully save album details to {output_file} from album: {album_id}')

    def crawler_track(self, album_id_list: list, output_file: str, threads: int = 10):
        with ThreadPoolExecutor(threads) as t:
            for album_id in album_id_list:
                t.submit(self._crawl_track, album_id=album_id, output_file=output_file)

    def _crawl_track(self, album_id, output_file: str):
        track_details = self.parser.parse_track(album_id)
        self.save_csv(track_details, output_file=output_file)
        print(f'Successfully save track details to {output_file} from album: {album_id}')

    def downloader_track(self, urls: list, download_names: list, threads: int = 10):
        with ThreadPoolExecutor(threads) as t:
            for url, download_name in zip(urls, download_names):
                t.submit(self._download_track, url=url, download_name=download_name)

    def _download_track(self, url: str, download_name: str):
        response = requests.get(url, headers=self.headers)
        with open(f'{self.download_dir}/{download_name}.m4a', 'wb') as f:
            f.write(response.content)
            print(f'Successfully download track {download_name} to {self.download_dir}')


if __name__ == '__main__':
    num_of_threads = 10
    my_category = '有声书'
    my_subcategory = ['文学', '经典', '成长', '社科', '商业']

    my_csv_dir = r'XimalayaFM/data/'
    my_download_dir = r'XimalayaFM/download/'
    my_cate_output = r'data_album_basic.csv'
    my_album_output = r'data_album_details.csv'
    my_track_output = r'data_tracks.csv'

    my_filters = {'announcer': 'single', 'finished': 'yes', 'paid': 'yes'}

    crawler = XimalayaFMCrawler(output_dir=my_csv_dir, download_dir=my_download_dir)

    crawler.crawler_category(category=my_category, subcategories=my_subcategory, filters=my_filters,
                             pages=4, threads=num_of_threads, output_file=my_cate_output)

    data_category = pd.read_csv(my_csv_dir + my_cate_output)
    crawler.crawler_album(data_category['album_id'][0:100], threads=num_of_threads, output_file=my_album_output)

    crawler.crawler_track(data_category['album_id'][0:50], threads=num_of_threads, output_file=my_track_output)

    data_tracks = pd.read_csv(my_csv_dir + my_track_output)
    data_trial_tracks = data_tracks[data_tracks['track_audio'].notnull()].reset_index(drop=True)[0:30]
    data_trial_tracks['download_name'] = \
        data_trial_tracks['album_id'].astype(str) + '_' + data_trial_tracks['track_id'].astype(str)
    crawler.downloader_track(data_trial_tracks['track_audio'], download_names=data_trial_tracks['download_name'],
                             threads=num_of_threads)
