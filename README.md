# Python Web Crawler 

## Baidu Search

### Baidu News 百度资讯
1. **Basic Usage**: `BaiduCrawler(cookie).search_news(words='search word', pages=3)`
2. **Params**:
    ```python
   """
   cookie: str. your cookie after login.
   words: list. a list of words used to search.
   pages: int. how many pages you want to crawl.
   output_path: str or None. Path used to save data. Defaults to None.
   """
3. **Return**: a `pandas.DataFrame` of results.
    ```python
   results = {
        'title': '网页标题', 
        'abstract': '网页摘要',
        'url': '网页网址',
        'source': '网页来源',
        'date': '发布日期',
        'crawl_time': '爬取时间',
        'search_word': '搜索词'
    }
    ```

### TODO
1. Baidu Web
2. Baidu Pic

## Bilibili

1. Bilibili 视频评论。单个视频下的评论数据。返回 评论id、评论时间、母评论id、评论点赞数、用户id、用户名称、评论内容。