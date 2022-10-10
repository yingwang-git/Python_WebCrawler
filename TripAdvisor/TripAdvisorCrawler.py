# -*- coding: utf-8 -*-
# Author: Ying Wang
# Date: 2022/10/10

class BaseTripAdvisor:
    def __init__(self):
        self.name = 'TripAdvisor Base Crawler'


class TripAdvisorParser(BaseTripAdvisor):
    def __init__(self):
        super().__init__()
        self.name = 'TripAdvisor Parser'


class TripAdvisorCrawler(BaseTripAdvisor):
    def __init__(self):
        super().__init__()
        self.name = 'TripAdvisor Crawler'
