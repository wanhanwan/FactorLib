from .stocklist_manager import StockListManager
from single_factor_test.config import parse_config
from utils import AttrDict
import pandas as pd
import os
import shutil


class StrategyManager(object):
    fields = ['id', 'name', 'researcher', 'latest_rebalance_date', 'stocklist_name', 'stocklist_id','stockpool',
              'benchmark', 'first_rebalance_date', 'rebalance_frequence']

    def __init__(self, strategy_path, stocklist_path):
        self._strategy_path = strategy_path
        self._stocklist_path = stocklist_path
        self._strategy_dict = None
        self._stocklist_manager = StockListManager(self._stocklist_path)
        self._init()

    # 初始化
    def _init(self):
        if not os.path.isdir(self._strategy_path):
            os.mkdir(self._strategy_path)
            self._strategy_dict = pd.DataFrame(columns=self.fields)
            self._strategy_dict.to_csv(os.path.join(self._strategy_path, 'summary.csv'))
        if not os.path.isfile(os.path.isfile(self._strategy_path, 'summary.csv')):
            self._strategy_dict = pd.DataFrame(columns=self.fields)
            self._strategy_dict.to_csv(os.path.join(self._strategy_path, 'summary.csv'))
        self._strategy_dict = pd.read_csv(os.path.isfile(self._strategy_path, 'summary.csv'))
        self._stocklist_manager._init()

    # 保存信息
    def _save(self):
        self._strategy_dict.to_csv(os.path.join(self._strategy_path,'summary.csv'))

    # 最大策略ID
    @property
    def _maxid(self):
        if not self._strategy_dict.empty():
            return self._strategy_dict['ID'].max()
        else:
            return 0

    # 策略对应的股票列表名称和ID
    def strategy_stocklist(self, strategy_id, strategy_name):
        if strategy_id is not None:
            return self._strategy_dict[self._strategy_dict.ID == strategy_id][['StockListName', 'StockListID']]
        elif strategy_name is not None:
            return self._strategy_dict[self._strategy_dict.Name == strategy_name][['StockListName', 'StockListID']]
        else:
            raise KeyError("No strategy identifier is provided")

    # 策略ID对应的策略名称
    def strategy_name(self, strategy_id=None):
        if strategy_id is not None:
            return self._strategy_dict[self._strategy_dict.Name == strategy_id]['Name']

    # 策略是否已存在
    def if_exists(self, name):
        return name in self._strategy_dict['Name'].tolist()

    # 从一个.yaml文件中创建一个策略
    def create_from_yml(self, file_path, if_exists='error', **kwargs):
        strategy_config = AttrDict(parse_config(file_path))
        for k, v in kwargs.items():
            if hasattr(strategy_config, k):
                setattr(strategy_config, k, v)
        if self.if_exists(strategy_config.name):
            if if_exists == 'error':
                raise KeyError("strategy %s already exists"%strategy_config.name)
            elif if_exists == 'replace':
                self.delete(name=strategy_config.name)
        strategy_path = os.path.join(self._strategy_path, strategy_config.name)
        # 创建策略文件夹
        os.mkdir(strategy_path)
        # 复制初始股票列表
        shutil.copy(strategy_config.initial_stocklist_path, strategy_path)
        self._stocklist_manager.add_new_one(strategy_config.initial_stocklist_path)
        # 复制中间数据
        if strategy_config.initial_temp_data_path is not None:
            shutil.copy(strategy_config.initial_temp_data_path, strategy_path)
        # 复制股票列表更新程序
        if strategy_config.updater_path is not None:
            shutil.copy(strategy_config.updater_path, strategy_path)
        # 策略的调仓日期
        stocklist_name = os.path.split(strategy_config.initial_stocklist_path)[1].replace('.csv', '')
        first_rebalance_date = self._stocklist_manager.min_rebalance_date(stocklist_name)
        latest_rebalance_date = self._stocklist_manager.max_rebalance_date(stocklist_name)
        # 添加新的记录
        self._add_record(stocklist_name=stocklist_name, first_rebalance_date=first_rebalance_date,
                         latest_rebalance_date=latest_rebalance_date, **strategy_config.__dict__)

    # 更新策略股票持仓
    def update_stocks(self, strategy_name, start, end):



    # 添加一条记录
    def _add_record(self, **kwargs):
        record = pd.DataFrame([None]*len(self.fields), columns=self.fields)
        record['id'] = self._maxid + 1
        record['stocklist_id'] = record['id']
        for k, v in kwargs.items():
            if k in self.fields:
                record[k] = v
        self._strategy_dict = self._strategy_dict.append(record)
        self._save()

    # 删除一个策略
    def delete(self, name=None, strategy_id=None):
        if name is not None:
            os.remove(os.path.join(self._stocklist_path, name))
            self._strategy_dict = self._strategy_dict[self._strategy_dict.Name != name]
            self._stocklist_manager.delete_stocklist([self.strategy_stocklist(strategy_name=name)])
        elif strategy_id is not None:
            name = self.strategy_name(strategy_id)
            os.remove(os.path.join(self._stocklist_path, name))
            self._strategy_dict = self._strategy_dict[self._strategy_dict.Name != name]
            self._stocklist_manager.delete_stocklist([self.strategy_stocklist(strategy_id=strategy_id)])
        else:
            self._save()
            raise KeyError("No strategy identifier is provided")
        self._save()

