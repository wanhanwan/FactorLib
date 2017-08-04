from utils.datetime_func import DateStr2Datetime
import importlib.util
import pandas as pd
import os
import shutil


class StockListManager(object):

    def __init__(self, path):
        self._stocklist_path = path

    def min_rebalance_date(self, name):
        f = open(os.path.join(self._stocklist_path, name+'.csv'))
        return pd.read_csv(f)['date'].min()

    def max_rebalance_date(self, name):
        f = open(os.path.join(self._stocklist_path, name+'.csv'))
        return pd.read_csv(f)['date'].max()

    def check_file_exists(self, name):
        return name+'.csv' in os.listdir(self._stocklist_path)

    def add_new_one(self, src):
        if self.check_file_exists(os.path.split(src)[1].replace('.csv', '')):
            raise FileExistsError
        shutil.copy(src, self._stocklist_path)
