from utils.stocklist_manager import StockListManager
from single_factor_test.config import parse_config
from utils import AttrDict
from datetime import datetime
from data_source import tc
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
        if not os.path.isfile(os.path.join(self._strategy_path, 'summary.csv')):
            self._strategy_dict = pd.DataFrame(columns=self.fields)
            self._strategy_dict.to_csv(os.path.join(self._strategy_path, 'summary.csv'), index=False)
        self._strategy_dict = pd.read_csv(os.path.join(self._strategy_path, 'summary.csv'))

    # 保存信息
    def _save(self):
        self._strategy_dict.to_csv(os.path.join(self._strategy_path, 'summary.csv'), index=False, quoting=3)

    # 最大策略ID
    @property
    def _maxid(self):
        if not self._strategy_dict.empty:
            return self._strategy_dict['id'].max()
        else:
            return 0

    # 策略对应的股票列表名称和ID
    def strategy_stocklist(self, strategy_id=None, strategy_name=None):
        if strategy_id is not None:
            return self._strategy_dict[self._strategy_dict.ID == strategy_id][['stocklist_name', 'stockList_id']]
        elif strategy_name is not None:
            return self._strategy_dict[self._strategy_dict.name == strategy_name][['stocklist_name', 'stockList_id']]
        else:
            raise KeyError("No strategy identifier is provided")

    # 策略ID对应的策略名称
    def strategy_name(self, strategy_id=None):
        if strategy_id is not None:
            return self._strategy_dict[self._strategy_dict.Name == strategy_id]['Name']

    # 策略是否已存在
    def if_exists(self, name):
        return name in self._strategy_dict['name'].tolist()

    # 从一个文件夹中创建一个策略
    def create_from_directory(self, src, if_exists='error'):
        cwd = os.getcwd()
        os.chdir(src)
        strategy_config = AttrDict(parse_config(os.path.join(src, 'config.yml')))
        if self.if_exists(strategy_config.name):
            if if_exists == 'error':
                raise KeyError("strategy %s already exists"%strategy_config.name)
            elif if_exists == 'replace':
                self.delete(name=strategy_config.name)
        strategy_path = os.path.join(self._strategy_path, strategy_config.name)
        # 创建策略文件夹
        os.mkdir(strategy_path)
        # 复制初始股票列表
        stocklist_filename = strategy_config.stocklist.output
        shutil.copy(stocklist_filename, strategy_path)
        self._stocklist_manager.add_new_one(os.path.abspath(stocklist_filename))
        # 复制中间数据
        if os.path.isdir('temp'):
            shutil.copytree('temp', os.path.join(strategy_path, 'temp'))
        # 复制股票列表更新程序
        if os.path.isdir('update'):
            shutil.copytree('update', os.path.join(strategy_path, 'update'))
        # 策略的调仓日期
        stocklist_name = stocklist_filename.replace('.csv', '')
        first_rebalance_date = self._stocklist_manager.min_rebalance_date(stocklist_name)
        latest_rebalance_date = self._stocklist_manager.max_rebalance_date(stocklist_name)
        # 添加新的记录
        self._add_record(stocklist_name=stocklist_name, first_rebalance_date=first_rebalance_date,
                         latest_rebalance_date=latest_rebalance_date, benchmark=strategy_config.stocklist.benchmark,
                         **strategy_config.__dict__)
        os.chdir(cwd)

    # 更新策略股票持仓
    def update_stocks(self, strategy_name, start, end):
        return

    # 更改策略属性
    def modify_attributes(self, strategy_id, **kwargs):
        for k, v in kwargs.items():
            if k in self._strategy_dict.columns.values:
                self._strategy_dict.loc[self._strategy_dict.id == strategy_id, k] = v
        self._save()

    # 添加一条记录
    def _add_record(self, **kwargs):
        record = pd.DataFrame([[None]*len(self.fields)], columns=self.fields)
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
            shutil.rmtree(os.path.join(self._strategy_path, name))
            self._strategy_dict = self._strategy_dict[self._strategy_dict.name != name]
            self._stocklist_manager.delete_stocklist([self.strategy_stocklist(strategy_name=name)])
        elif strategy_id is not None:
            name = self.strategy_name(strategy_id)
            shutil.rmtree(os.path.join(self._stocklist_path, name))
            self._strategy_dict = self._strategy_dict[self._strategy_dict.name != name]
            self._stocklist_manager.delete_stocklist([self.strategy_stocklist(strategy_id=strategy_id)])
        else:
            self._save()
            raise KeyError("No strategy identifier is provided")
        self._save()

    # 策略最近的股票
    def latest_position(self, strategy_name=None, strategy_id=None):
        stocklist_info = self.strategy_stocklist(strategy_id, strategy_name)
        if strategy_name is not None:
            max_date = self._strategy_dict.loc[self._strategy_dict.name==strategy_name, 'latest_rebalance_date']
        else:
            max_date = self._strategy_dict[self._strategy_dict.id==strategy_id, 'latest_rebalance_date']
        return self._stocklist_manager.get_position(stocklist_info['stocklist_name'], max_date)

    # 生成交易指令
    def generate_tradeorder(self, strategy_id, capital, realtime=False):
        stocks = self.latest_position(strategy_id=strategy_id)
        """如果当前是交易时间，"""
        if tc.is_trading_time(datetime.now()):

        return


if __name__ == '__main__':
    sm = StrategyManager('D:/data/factor_investment_strategies', 'D:/data/factor_investment_stocklists')
    # sm.delete(name="GMTB")
    sm.create_from_directory('D:/data/factor_investment_temp_strategies/GMTB')