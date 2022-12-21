# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/12/19

import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests


class XimalayaFMCrawler:
    def __init__(self, db_path=None, download_dir=None):
        self.name = 'XimalayaFM Crawler'
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'}
        self.db_path = db_path
        self.download_dir = download_dir

    def get_category(self, category: str, subcategory: str = None, page: int = 1, filters: dict = None,
                     get_total_pages: bool = False):
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
        if filters and category_code == 'youshengshu':
            # only 'youshengshu' can be filtered by announcer and serialize state (probably)
            params['meta'] = '-'.join([self._format_filters(k, v) for k, v in filters.items()])
        if filters and category_code != 'youshengshu':
            # most categories can be filtered by paid type
            params['meta'] = self._format_filters('paid', filters['paid'])
        if 'metadataValues not find' in requests.get(cate_url, headers=self.headers, params=params).text:
            # some categories have no filters
            del params['meta']

        category_json = json.loads(requests.get(cate_url, headers=self.headers, params=params).text)

        total_pages = category_json['data']['total'] // 50 + 1
        if get_total_pages:
            return total_pages if total_pages < 50 else 50

        albums = []
        for album in category_json['data']['albums']:
            album_basic = {
                'album_id': album['albumId'],
                'album_paid': album['isPaid'],  # is paid. True = need paid or VIP, False = free
                'album_finished': album['isFinished'],  # is finished. 0 = not available, 1 = serialized, 2 = finished
                'album_vipType': album['vipType'],  # paid type. 0 = only paid, 1 = only VIP, 2 = VIP or paid
                'category': category,
                'subcategory': subcategory
            }
            albums.append(album_basic)
            if self.db_path:
                self._save2db([album_basic], table_name='album_basic', album_id=album['albumId'])
        return albums

    def get_album_detail(self, album_id):
        album_url = f'https://mobile.ximalaya.com/mobile/v1/album/ts-{round(time.time() * 1000)}?albumId={album_id}'
        album = self._get_json(album_url)['data']['album']
        details = [{
            **{'album_id': album_id,
               'album_title': album['title'],
               'album_subtitle': album.get('customSubTitle'),
               'album_info': album['intro'],  # TODO 部分不完整，有省略
               'album_tags': album.get('tags'),
               'album_cover': album['coverSmall'].split('!')[0],
               'album_score': album.get('score'),  # quality score (user evaluation, 0-5). not available for free albums
               'album_score_10': self._get_album_score(album['albumId']),  # popularity score (0-10). shown on the page
               'album_create': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(album['createdAt'] / 1000)),
               'album_tracks': album['tracks'],
               'album_plays': album['playTimes'],
               'album_comments': album.get('commentsCount'),
               'album_subscribes': album['subscribeCount']},
            **self._parse_album_price(album['albumId']),
            **self._parse_author(album['uid']),
            **self._parse_author_verify(album['uid'])
        }]
        if self.db_path:
            self._save2db(details, table_name='album_detail', album_id=album_id)
        return details

    def get_album_track(self, album_id):
        album_url = f'https://mobile.ximalaya.com/mobile/v1/album/track/ts-{round(time.time() * 1000)}?' \
                    f'albumId={album_id}&pageSize=50&pageId='
        track_json = self._get_json(album_url + '1')['data']

        track_details = []
        for page in range(1, track_json['maxPageId'] + 1):
            tracks = self._get_json(album_url + str(page))['data']['list']
            for track in tracks:
                track_detail = {
                    'album_id': album_id,
                    'track_id': track['trackId'],
                    'track_name': track['title'],
                    'track_duration': track['duration'],
                    'track_plays': track['playtimes'],
                    'track_likes': track['likes'],
                    'track_comments': track['comments'],
                    'track_create': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(track['createdAt'] / 1000)),
                    'track_audio': track['playUrl32'].replace(
                        'http://aod.cos.tx.xmcdn.com/', 'https://audiopay.cos.tx.xmcdn.com/download/1.0.0/')
                    # download url for free (trial) tracks. valid for hours.
                }
                track_details.append(track_detail)
                if self.db_path:
                    self._save2db([track_detail], table_name='album_track',
                                  album_id=album_id, track_id=track['trackId'])
        return track_details

    def crawler_category(self, category: str, subcategories: list = None,
                         filters: dict = None, pages: int = None, threads: int = 10):
        print(f' start to crawl albums from category {category} '.center(100, '='))
        results = []
        with ThreadPoolExecutor(threads) as t:
            for subcategory in subcategories:
                max_pages = self.get_category(category=category, subcategory=subcategory,
                                              filters=filters, get_total_pages=True)
                for page in range(1, pages + 1 if pages and pages <= max_pages else max_pages + 1):
                    # maximum 50 pages for each category
                    results.append(t.submit(self.get_category, category=category, subcategory=subcategory,
                                            filters=filters, page=page))
                    total_results = as_completed(results)
        return pd.DataFrame(sum([result.result() for result in total_results], []))

    @staticmethod
    def crawler_threading(func, album_id_list: list, threads: int = 10):
        print(f' start to run {func.__name__} '.center(100, '='))
        results = []
        with ThreadPoolExecutor(threads) as t:
            for album_id in album_id_list:
                results.append(t.submit(func, album_id=album_id))
                total_results = as_completed(results)
        return pd.DataFrame(sum([result.result() for result in total_results], []))

    def _get_json(self, url: str):
        response = requests.get(url, headers=self.headers)
        return json.loads(response.text)

    def _get_categories_map(self):
        categories_url = 'https://m.ximalaya.com/m-revision/page/category/queryCategories'
        categories = self._get_json(categories_url)['data']
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
        # pd.DataFrame(category_list).to_csv(r'XimalayaFM/map_categories.csv', index=False, encoding='utf-8-sig')
        return pd.DataFrame(category_list)

    def _format_category(self, category: str, subcategory: str = None):
        categories = self._get_categories_map()
        cate = categories[['category_name', 'category_code']]
        if category.lower() not in cate.values:
            raise ValueError(f'ERROR: category {category} not exist! Please check!')
        category_code = (category.lower() if cate.columns[cate.eq(category.lower()).any()][0] == 'category_code'
                         else cate.loc[cate['category_name'] == category, 'category_code'].iloc[0])
        if not subcategory:
            return category_code, ''
        _ = categories.loc[categories['category_code'] == category_code].reset_index(drop=True)
        subcate = _[['subcategory_name', 'subcategory_code']]
        if subcategory.lower() not in subcate.values:
            raise ValueError(f'ERROR: subcategory {subcategory} not exist in category {category}! Please check!')
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
            raise ValueError("ERROR: invalid filter! 'announcer' should be 'single', 'double',or 'multiple'; "
                             "'finished' should be 'no' or 'yes'; 'paid' should be 'no' or 'yes' ")
        return filter_map[key.lower()][value]

    def _get_album_score(self, album_id):
        score_url = f'https://www.ximalaya.com/revision/comment/albumStatistics/{album_id}'
        return self._get_json(score_url)['data'].get('albumScore')  # popularity score. 0-10

    def _parse_album_price(self, album_id):
        price_url = f'https://www.ximalaya.com/revision/bdsp/album/pay/schema?id={album_id}&productType=1'
        price = self._get_json(price_url)['data']['albumPrice']

        if price.get('retailAlbum'):
            album_price = price['retailAlbum']['unBroughtTotalAmount']
        elif price.get('wholeAlbum'):
            album_price = price['wholeAlbum']['price']
        else:
            album_price = None

        return {
            'album_paid_type': price['paidType'],  # paid type. 0 = VIP or free (no price), 1 = by album, 2 = by track
            'album_price': album_price,
            'album_price_single': None if not price.get('retailAlbum') else price['retailAlbum']['singlePrice']
        }

    def _parse_author(self, author_id):
        author_url = f'https://www.ximalaya.com/revision/user/basic?uid={author_id}'
        author = self._get_json(author_url)['data']
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
        verify = self._get_json(verify_url)['data']['anchorInfo']['userInfo']
        return {
            'author_verified': verify['verifyStatus'],  # is verified. 1 = not verified, 3 = verified
            'author_verified_type': verify['verifyType'],  # verified type. 1 = person, 2 = company
            'author_verified_desc': verify.get('ptitle')
        }

    def _create_db(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cur = conn.cursor()

        # table: album_basic
        cur.execute("""
            CREATE TABLE IF NOT EXISTS album_basic (
            album_id INTEGER PRIMARY KEY,
            album_paid TEXT, /* is paid. True=need paid or VIP, False=free */
            album_finished INTEGER,  /* is finished. 0=not available, 1=serialized, 2=finished */
            album_vipType INTEGER,  /* paid type. 0=only paid, 1=only VIP, 2=VIP or paid */
            category TEXT,
            subcategory TEXT
            )
        """)

        # table: album_detail
        cur.execute("""
            CREATE TABLE IF NOT EXISTS album_detail (
            album_id INTEGER PRIMARY KEY,
            album_title TEXT,
            album_subtitle TEXT,
            album_info TEXT,
            album_tags TEXT,
            album_cover TEXT, 
            album_score REAL, /* quality score (user evaluation, 0-5). not available for free albums */
            album_score_10 REAL, /* popularity score (0-10). shown on the page */
            album_create TEXT,
            album_tracks INTEGER,
            album_plays INTEGER,
            album_comments INTEGER,
            album_subscribes INTEGER,
            album_paid_type INTEGER, /* paid type. 0=VIP or free (no price), 1=by album, 2=by track */
            album_price REAL, 
            album_price_single REAL,
            author_id INTEGER,
            author_name TEXT,
            author_gender INTEGER,
            author_level INTEGER,
            author_vip INTEGER,
            author_signature TEXT, 
            author_desc TEXT,
            author_headimg TEXT, 
            author_following INTEGER,
            author_followers INTEGER,
            author_albums INTEGER,
            author_tracks INTEGER,
            author_verified INTEGER, /* is verified. 1 = not verified, 3 = verified */
            author_verified_type INTEGER, /* verified type. 1 = person, 2 = company */
            author_verified_desc TEXT
            )
        """)

        # table: album_track
        cur.execute("""
            CREATE TABLE IF NOT EXISTS album_track (
            album_id INTEGER,
            track_id INTEGER PRIMARY KEY,
            track_name TEXT,
            track_duration INTEGER, 
            track_plays INTEGER, 
            track_likes INTEGER, 
            track_comments INTEGER, 
            track_create TEXT,
            track_audio TEXT
            )
        """)

        cur.close()

    def _save2db(self, result: list, table_name, album_id='', track_id=''):
        self._create_db()
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cur = conn.cursor()
        try:
            pd.DataFrame(result).to_sql(name=table_name, con=conn, if_exists='append', index=False)
            print(f'successfully save result {album_id} {track_id}')
        except sqlite3.IntegrityError:
            print(f'{album_id} {track_id} result already exist in database, pass!')
            pass
        finally:
            cur.close()
            conn.close()

    def downloader_track(self, urls: list, download_names: list, threads: int = 10):
        print(' start to download tracks '.center(100, '='))
        with ThreadPoolExecutor(threads) as t:
            for url, download_name in zip(urls, download_names):
                t.submit(self._download_track, url=url, download_name=download_name)

    def _download_track(self, url: str, download_name: str):
        response = requests.get(url, headers=self.headers)
        if not self.download_dir:
            raise AttributeError('ERROR: please set download directory before downloading audios')
        with open(f'{self.download_dir}/{download_name}.m4a', 'wb') as f:
            f.write(response.content)
            print(f'Successfully download track {download_name} to {self.download_dir}')


if __name__ == '__main__':
    # settings
    num_threads = 10
    my_category = '有声书'
    my_subcategories = ['文学', '经典', '成长', '社科', '商业']
    # map_categories = pd.read_csv(r'XimalayaFM/data/map_categories.csv')
    # my_subcategories = list(map_categories.loc[map_categories['category_name'] == my_category, 'subcategory_name'])
    my_filters = {'announcer': 'single', 'paid': 'yes'}

    my_db_path = r'XimalayaFM\data\ximalaya.db'
    my_download_dir = r'XimalayaFM\download\\'

    crawler = XimalayaFMCrawler(db_path=my_db_path, download_dir=my_download_dir)
    df_basic = crawler.crawler_category(category=my_category, subcategories=my_subcategories,
                                        filters=my_filters, threads=num_threads)
    df_basic = df_basic.drop_duplicates(subset=['album_id']).reset_index(drop=True)
    df_details = crawler.crawler_threading(crawler.get_album_detail, album_id_list=df_basic['album_id'])
    df_tracks = crawler.crawler_threading(crawler.get_album_track, album_id_list=df_basic['album_id'])
