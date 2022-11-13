# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/11/7

import csv
import json
import time

import pandas as pd
import requests


class BaseXimalayaFM:
    def __init__(self, output_dir, download_dir):
        self.name = 'XimalayaFM Base Crawler'
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'}
        self.output_dir = output_dir  # directory saving csv results
        self.download_dir = download_dir  # directory saving downloaded audios

    def get_album_by_category(self, category: str, subcategory: str = None,
                                 page: int = 1, announcers: str = None):
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

        if category_code == 'youshengshu' and announcers:
            params['meta'] = self._format_announcers(announcers)

        cate_response = requests.get(cate_url, headers=self.headers, params=params)
        return json.loads(cate_response.text)['data']['albums']

    def _get_categories(self):
        categories_url = 'https://m.ximalaya.com/m-revision/page/category/queryCategories'
        categories = self._get_json(categories_url)['data']
        category_list = []
        for item in categories:
            category = item['category']
            category_details = {
                'category_id': category['categoryId'],
                'category_name': category['displayName'],
                'category_code': category['code'],
                'category_link': category['link']
            }
            for subcategory in category['subCategories']:
                subcategory_details = {
                    'subcategory_id': subcategory['subCategoryId'],
                    'subcategory_name': subcategory['displayValue'],
                    'subcategory_code': subcategory['code'],
                    'subcategory_link': subcategory['link']
                }
                category_list.append({**category_details, **subcategory_details})
        # pd.DataFrame(category_list).to_csv(r'XimalayaFM/dict_categories.csv', index=False, encoding='utf-8-sig')
        return pd.DataFrame(category_list)

    def _format_category(self, category: str, subcategory: str = None):
        categories = self._get_categories()
        # categories = pd.read_csv(r'XimalayaFM/dict_categories.csv')
        cate = categories[['category_name', 'category_code']]
        if category in cate.values:
            category_code = (category if cate.columns[cate.eq(category).any()][0] == 'category_code'
                             else cate.loc[cate['category_name'] == category, 'category_code'][0])
            focus_category = categories.loc[categories['category_code'] == category_code].reset_index(drop=True)

            if subcategory:
                subcate = focus_category[['subcategory_name', 'subcategory_code']]
                if subcategory in subcate.values:
                    subcate_code = (
                        subcategory if subcate.columns[subcate.eq(subcategory).any()][0] == 'subcategory_code'
                        else subcate.loc[subcate['subcategory_name'] == subcategory, 'subcategory_code'][0])
                    return category_code, subcate_code
                else:
                    print(f'ERROR: subcategory {subcategory} not exist in category {category}! Please check!')
            else:
                return category_code, ''
        else:
            print(f'ERROR: {category} not exist! Please check!')

    def _get_json(self, url):
        response = requests.get(url, headers=self.headers)
        return json.loads(response.text)

    def _save_csv(self, result, output_file: str = None, csv_header: list = None):
        if output_file:
            with open(f'{self.output_dir}/{output_file}', 'a', newline='', encoding='utf-8-sig') as fp:
                csv_writer = csv.DictWriter(fp, csv_header)
                if fp.tell() == 0:
                    csv_writer.writeheader()
                csv_writer.writerows(result)

    @staticmethod
    def _format_announcers(announcers: str):
        if announcers == 'single':
            return '272_4361'
        elif announcers == 'double':
            return '272_4362'
        elif announcers == 'multiple':
            return '272_4363'
        else:
            print("ERROR: invalid statement! announcers should be 'single', 'double',or 'multiple'")


class XimalayaFMParser(BaseXimalayaFM):
    def __init__(self, output_dir, download_dir):
        super().__init__(output_dir, download_dir)
        self.name = 'XimalayaFM Parser'

    def parser_album(self, cate_json):
        cate_items = []
        for item in cate_json['data']['albums']:
            cate_items.append({
                **{'album_id': item['albumId'],
                   'album_paid': item['isPaid'],  # 是否付费。True=需会员或付费，False=免费
                   'album_finished': item['isFinished'],  # 是否完结。0=无此属性，1=连载，2=完结
                   'album_vipType': item['vipType']},  # 付费类型。0=仅付费，1=仅会员，2=会员或付费
                **self._parser_album_details(item['albumId']),
                **self._parser_album_price(item['albumId']),
                **self._parser_author(item['uid']),
                **self._parser_author_verify(item['uid'])
            })

        csv_header = ['album_id', 'album_paid', 'album_finished', 'album_vipType',
                      'album_title', 'album_subtitle', 'album_info', 'album_tags', 'album_cover',
                      'album_score', 'album_score_10', 'album_create', 'album_tracks',
                      'album_plays', 'album_comments', 'album_subscribes',
                      'album_paid_type', 'album_price', 'album_price_single',
                      'author_id', 'author_name', 'author_gender', 'author_level', 'author_vip', 'author_signature',
                      'author_desc', 'author_headimg', 'author_following', 'author_followers', 'author_albums',
                      'author_tracks', 'author_verified', 'author_verified_type', 'author_verified_desc']

        return cate_items, csv_header

    def _parser_album_details(self, album_id):
        album_url = f'https://mobile.ximalaya.com/mobile/v1/album/ts-{round(time.time() * 1000)}?albumId={album_id}'
        album = crawler._get_json(album_url)['data']['album']

        album_details = [{
            'album_title': album['title'],
            'album_subtitle': album['customSubTitle'],
            'album_info': album['intro'],
            'album_tags': album['tags'],
            'album_cover': album['coverSmall'].split('!')[0],
            'album_score': album['score'],
            'album_score_10': self._parser_album_score(album['albumId']),
            'album_create': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(album['createdAt'] / 1000)),
            'album_tracks': album['tracks'],
            'album_plays': album['playTimes'],
            'album_comments': album['commentsCount'],
            'album_subscribes': album['subscribeCount']
        }]

        return album_details

    def _parser_album_price(self, album_id):
        price_url = f'https://www.ximalaya.com/revision/bdsp/album/pay/schema?id={album_id}&productType=1'
        price = self._get_json(price_url)['data']['albumPrice']
        if price['isPaid']:
            if price['paidType'] == 0:
                album_price = None
            elif price['paidType'] == 1:
                album_price = price['wholeAlbum']['price']
            else:
                album_price = price['retailAlbum']['unBroughtTotalAmount']
        else:
            album_price = None

        price_detail = {
            'album_paid_type': price['paidType'],  # 价格类型。0=vip专享（无价格），1=整张购买，2=分集购买
            'album_price': album_price,
            'album_price_single': (price['retailAlbum']['singlePrice'] if price.__contains__('singlePrice') else None)
        }
        return price_detail

    def _parser_album_score(self, album_id):
        score_url = f'https://www.ximalaya.com/revision/comment/albumStatistics/{album_id}'
        # if 'albumScore' in self._get_json(score_url)['data'].keys():  # 10分制得分，页面显示的得分
        if self._get_json(score_url)['data'].__contains__('albumScore'):
            return self._get_json(score_url)['data']['albumScore']
        else:
            return None

    def _parser_author(self, author_id):
        author_url = f'https://www.ximalaya.com/revision/user/basic?uid={author_id}'
        author = self._get_json(author_url)['data']
        author_details = {
            'author_id': author['uid'],
            'author_name': author['nickName'],
            'author_gender': author['gender'],
            'author_level': author['anchorGrade'],
            'author_vip': author['isVip'],
            'author_signature': (author['personalSignature'] if author.__contains__('personalSignature') else None),
            'author_desc': (author['personalDescription'] if author.__contains__('personalDescription') else None),
            'author_headimg': 'https' + author['cover'],
            'author_following': author['followingCount'],
            'author_followers': author['fansCount'],
            'author_albums': author['albumsCount'],
            'author_tracks': author['tracksCount']
        }
        return author_details

    def _parser_author_verify(self, author_id):
        verify_url = f'https://m.ximalaya.com/m-revision/page/anchor/queryAnchorPage/{author_id}'
        verify = self._get_json(verify_url)['data']['anchorInfo']['userInfo']
        verify_details = {
            'author_verified': verify['verifyStatus'],
            'author_verified_type': verify['verifyType'],
            'author_verified_desc': (verify['ptitle'] if verify.__contains__('ptitle') else None)
        }
        return verify_details

    def parser_track(self, album_id, download: bool = True):
        album_url = f'https://mobile.ximalaya.com/mobile/v1/album/track/ts-{round(time.time() * 1000)}?' \
                    f'albumId={album_id}&pageSize=50&pageId='
        track_json = self._get_json(album_url + '1')['data']

        track_details = []
        for page in range(1, track_json['maxPageId'] + 1):
            tracks = self._get_json(album_url + str(page))['data']['list']
            for track in tracks:
                if track['playUrl32']:
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
                    if download:
                        self._download_track(track['playUrl32'], download_name=f"{album_id}_{track['trackId']}")
                else:
                    # print('non-sample track!')
                    pass

        csv_header = ['album_id', 'track_id', 'track_name', 'track_duration', 'track_plays', 'track_likes',
                      'track_comments', 'track_create', 'track_audio']
        return track_details, csv_header

    def _download_track(self, url, download_name):
        response = requests.get(url, headers=self.headers)
        with open(f'{self.download_dir}/{download_name}.m4a', 'wb') as f:
            f.write(response.content)
            print(f'Successfully download track {download_name} to {self.download_dir}')


class XimalayaFMCrawler(BaseXimalayaFM):
    def __init__(self, output_dir, download_dir):
        super().__init__(output_dir, download_dir)
        self.name = 'XimalayaFM Crawler'
        self.parser = XimalayaFMParser(output_dir, download_dir)

    def crawl_category(self, category: str, subcategory: str = None, announcers: str = None,
                       output_file: str = None):
        all_items = []
        total = self.get_album_by_category(page=1, category=category, subcategory=subcategory,
                                           announcers=announcers)['data']['total']
        for page in range(1, (total // 50) + 1 if total < 2500 else 51):  # maximum 50 pages per query
            page_json = self.get_album_by_category(category=category, subcategory=subcategory,
                                                   page=page, announcers=announcers)
            page_items, csv_header = self.parser.parser_album(page_json)
            all_items.extend(page_items)

            if output_file:
                self._save_csv(page_items, output_file, csv_header)
                print(f'Successfully save category result to {output_file} from category: {category}, page: {page}')
            else:
                print(f'Successfully get category result from from category: {category}, page: {page}')

        return pd.DataFrame(all_items)

    def crawl_album(self, album_id_list: list = None, output_file: str = None):
        all_details = []
        for album_id in album_id_list:
            album_detail, csv_header = self.parser.parser_album(album_id)
            all_details.extend(album_detail)

            if output_file:
                self._save_csv(album_detail, output_file, csv_header)
                print(f'Successfully save album details to {output_file} from album {album_id}')
            else:
                print(f'Successfully get details from from album {album_id}')

        return pd.DataFrame(all_details)

    def crawl_track(self, album_id_list: list = None, output_file: str = None, download: bool = True):
        all_tracks = []
        for album_id in album_id_list:
            track_detail, csv_header = self.parser.parser_track(album_id, download=download)
            all_tracks.extend(track_detail)

            if output_file:
                self._save_csv(track_detail, output_file, csv_header)
                print(f'Successfully save track details to {output_file} from album: {album_id}')
            else:
                print(f'Successfully get tracks from from album {album_id}')

        return pd.DataFrame(all_tracks)


if __name__ == '__main__':
    my_category = 'shangye'
    my_subcategory = 'shangjie'
    my_filters = {'free': 'no'}

    my_csv_dir = r'XimalayaFM/data/'
    my_download_dir = r'XimalayaFM/download/'
    my_cate_output = r'data_category.csv'
    my_album_output = r'data_albums.csv'
    my_track_output = r'data_tracks.csv'
    #
    # crawler = BaseXimalayaFM(my_csv_dir, my_download_dir)
    # cate_json = pd.DataFrame(crawler.get_basic_album_category(category='shangye', subcategory='商界'))

    crawler = XimalayaFMCrawler(output_dir=my_csv_dir, download_dir=my_download_dir)

    data_category = crawler.crawl_category(category=my_category, subcategory=my_subcategory,
                                           output_file=my_cate_output)
    data_albums = crawler.crawl_album(album_id_list=data_category['album_id'], output_file=my_album_output)
    data_tracks = crawler.crawl_track(album_id_list=data_albums['album_id'], output_file=my_track_output,
                                      download=True)
