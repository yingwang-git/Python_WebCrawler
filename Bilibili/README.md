# Bilibili

1. crawl videos from a bilibili channel
2. crawl video details and uploader information from video(s)
3. crawl video comments from video(s)
4. crawl video bullet from video(s)

> **Note**: The above four result tables (`pandas.DataFrame`), use the `bvid` (视频 bv 号) as the key to relate each other.

## crawl a channel
1. **Description**: crawl all recent (uploaded in last seven days) videos from a bilibili channel.  
2. **Basic Usage**
   ```
   BilibiliCrawler().crawl_channel(channel_url='https://www.bilibili.com/v/knowledge/science')
   ```
4. **Params**
   ```
   channel_url: str. the url of a bilibili channel. e.g., “知识-科学科普”（https://www.bilibili.com/v/knowledge/science）
   output_path: str. the path to save the csv results.
   ```
5. **Return**: a `pandas.DataFrame` of results.
   ```
   channel_results = {
   'bvid': 'video bvid, 视频 bv 号',
   'url': 'video url, 视频网址',
   'tags': 'video tags, 视频标签',
   'channel_crawl_time': 'crawl time, 爬取时间'
   }
   ```

## crawl videos
1. **Description**: crawl video details and uploader information from video(s).  
2. **Basic Usage**
   ```
   BilibiliCrawler().crawl_video(bvid_list=['BV16X4y1g7wT'])
   ```
4. **Params**
   ```
   bvid_list: list. a list of bvid. Note that one bvid should also be in a list.
   output_path: str. the path to save the csv results.
   ```
5. **Return**: a `pandas.DataFrame` of results.
   ```
   video_results = {
   # video info
   'bvid': 'video bvid, 视频 bv 号',
   'avid': 'video avid, 视频 av 号',
   'cid': 'video cid, 视频 cid 号',
   'title': 'video title, 视频标题',
   'pubdate': 'upload datetime, 视频上传时间',
   'duration': 'duration of the video (s), 视频时长（秒）',
   'views': 'number of views, 观看数',
   'likes': 'number of likes, 点赞数',
   'coins': 'number of coins, 投币数',
   'shares': 'number of shares, 分享数',
   'favorites': 'number of favorites, 收藏数',
   'bullets': 'number of bullets, 弹幕数',
   'comments': 'number of comments, 评论数',
   
   # uploader info
   'up_id': 'uploader user id, up主id',
   'up_name': 'uploader name, up主名称',
   'up_gender': 'uploader gender, up主性别',
   'up_fans': 'number of fans, up主粉丝数',
   'up_following': 'number of following, up主关注数',
   'up_level': 'level of the uploader, up主b站等级',
   'up_vip': 'vip info of the uploader, up主会员信息',
   'up_official': 'certification info of the uploader, up主认证信息',
   'up_archives': 'total number of videos uploaded by the uploader, up主总视频数',
   
   'video_crawl_time': 'crawl time, 爬取时间'
   }
   ```
   
## crawl comments
1. **Description**: crawl all comments from video(s).  
2. **Basic Usage**
   ```
   BilibiliCrawler().crawl_comment(bvid_list=['BV16X4y1g7wT'])
   ```
4. **Params**
   ```
   bvid_list: list. a list of bvid. Note that one bvid should also be in a list.
   output_path: str. the path to save the csv results.
   ```
5. **Return**: a `pandas.DataFrame` of results.
   ```
   comment_results = {
   'bvid': 'video bvid, 视频 bv 号',
   'comment_id': 'comment id, 评论 id',
   'comment_time': 'time post the comment, 评论时间',
   'comment_user_id': 'user id, 评论用户id',
   'comment_user_name': 'user name, 评论用户名称',
   'comment_content': 'comment content, 评论内容',
   'comment_likes': 'number of likes of the comment, 评论的点赞数',
   'comment_crawler_time': 'crawl time, 爬取时间'
   }
   ```
## crawl bullets

1. **Description**: crawl all bullets from video(s).  
2. **Basic Usage**
   ```
   BilibiliCrawler().crawl_bullet(bvid_list=['BV16X4y1g7wT'])
   ```
4. **Params**
   ```
   bvid_list: list. a list of bvid. Note that one bvid should also be in a list.
   output_path: str. the path to save the csv results.
   ```
5. **Return**: a `pandas.DataFrame` of results.
   ```
   comment_results = {
   'bvid': 'video bvid, 视频 bv 号',
   'bullet_content': 'bullet content, 弹幕内容',
   'bullet_entry': 'the seconds when the bullet enter the video, 弹幕进入视频的时间',
   'bullet_time': 'bullet post time, 弹幕发布时间',
   'bullet_crawler_time': 'crawl time, 爬取时间'
   }
   ```
