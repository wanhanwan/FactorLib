from utils.datetime_func import DateStr2Datetime
import importlib.util
import pandas as pd
import os


class StockListManager(object):
    fields = ['ID', 'Name', 'Researcher', 'LatestRebalanceDate', 'ExcelName', 'StockPool', 'Benchmark',
              'FirstRebalanceDate', 'RebalanceFrequence', 'StrategyPath']

    def __init__(self, path):
        self.path = path
        self.summarydb = os.path.join(path, 'summary.csv')
        if os.path.isfile(self.summarydb):
            self._read_db()
        else:
            self._init_db()
        self.conn = None

    def _init_db(self):
        init_df = pd.DataFrame(columns=self.fields)
        init_df.to_csv(self.summarydb)

    def _close_db(self):
        self.summarydata.to_csv(self.summarydb)
        self.summarydata = None

    def _maxid(self):
        if not self.summarydata.empty:
            return self.summarydata['ID'].max()
        else:
            return 0

    def _read_db(self):
        f = open(self.summarydb)
        df = pd.read_csv(f)
        self.summarydata = df

    def _add_record(self, new_record):
        self.summarydata = self.summarydata.append(new_record)
        self.summarydata.to_csv(self.summarydb)

    def _delete_record(self, IDs):
        if not isinstance(IDs, list):
            IDs = [IDs]
        self.summarydata = self.summarydata[~self.summarydata.IDs.isin(IDs)]
        self.summarydata.to_csv(self.summarydb)

    def _getattr(self, IDs, attrs):
        if not isinstance(IDs, list):
            IDs = [IDs]
        if attrs == 'all':
            attrs = self.fields
        return self.summarydata.loc[self.summarydata.IDs.isin(IDs), attrs]

    def _updateattrs(self, ID, **kwargs):
        old = self.summarydata.set_index(ID)
        new = pd.DataFrame(kwargs, index=[ID])
        self.summarydata = old.update(new).reset_index()
        self.summarydata.to_csv(self.summarydb)

    def _query(self, query_str, return_ids=False):
        if not return_ids:
            return self.summarydata.query(query_str)
        else:
            return self.summarydata.query(query_str)['IDs'].tolist()

    def _read_stocklist(self, ID):
        name = self._getattr(ID, 'ExcelName')
        f = open(os.path.join(self.path, name+'.csv'))
        stocklist = pd.read_csv(f, index_col=[0, 1])
        return stocklist

    def add_new(self, stocklist, name, stockpool, benchamrk, rebalancefreq, strategypath, researcher='wanshuai'):
        ID = self._maxid()+1
        excel_name = "%d_%s"%(ID, name)
        first_rebalance_date = stocklist.index.levels.get_level_values(0).min().strftime("%Y%m%d")
        latest_rebalance_date = stocklist.index.levels.get_level_values(0).max().strftime("%Y%m%d")
        stocklist.sort_index().to_csv(os.path.join(self.path, excel_name+'.csv'))
        record = pd.DataFrame([[ID, name, researcher, latest_rebalance_date, excel_name, stockpool,
                                benchamrk, first_rebalance_date, rebalancefreq, strategypath]], columns=self.fields)
        self._add_record(record)

    def append_stocklist(self, ID, stocklist):
        attrs = self._getattr(ID, ['ExcelName', 'LatestRebalanceDate'])
        old_stocklist = self._read_stocklist(ID)
        stocklist = stocklist[stocklist.index.get_level_values(0) > DateStr2Datetime(attrs['LatestRebalanceDate'])]
        new_stocklist = old_stocklist.append(stocklist)
        new_stocklist.sort_index().to_csv(os.path.join(self.path, attrs['ExcelName']+'.csv'))
        latest_rebalance_date = new_stocklist.index.get_level_values(0).max().strftime("%Y%m%d")
        self._updateattrs(ID, LatestRebalanceDate=latest_rebalance_date)

    def update_stocklist(self, ID, start, end):
        dirpath = self.summarydata.loc[self.summarydata.ID == ID, 'StrategyPath']
        if os.path.isfile(os.path.join(dirpath, 'update.py')):
            spec = importlib.util.spec_from_file_location('update', os.path.join(dirpath, 'update.py'))
            update = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(update)
            stocklist = update(start, end)
            self.append_stocklist(ID, stocklist)
        else:
            raise FileNotFoundError("File update.py not found")

    def delete_stocklist(self, IDs):
        excel_names = self._getattr(IDs, 'ExcelName')
        for name in excel_names.values:
            os.remove(os.path.join(self.path, name+'.csv'))
        self._delete_record(IDs)

    def get_positons(self, ID, dates):
        if not isinstance(dates, list):
            dates = [dates]
        stocklist = self._read_stocklist(ID)
        return stocklist.loc[dates]
