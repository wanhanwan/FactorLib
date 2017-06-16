"""
构建基于arctic的mongodb数据库，数据库的结构如下:
Library<<symbol<<dataset
其中，
Library：相当于关系型数据库中的表
symbol：相当于表中的字段，每一个Library包含若干个symbol，并且是单独存放的
dataset：pandas.DataFrame格式，索引=(date:DatetimeIndex,IDs:'******')

dataset索引构建规则：
1. 面板数据    index(date,IDs)
2. 时间序列数据 index(date)
"""

from arctic import Arctic, CHUNK_STORE, VERSION_STORE, TICK_STORE
#from utils.datetime_func import Datetime2DateStr
import pandas as pd


class mongoDB(object):
    LIBRARY_TYPE = {'chunk_store':CHUNK_STORE,'version_store':VERSION_STORE,
                    'tick_store':TICK_STORE}

    def __init__(self, host='localhost', library='stocks'):
        self._host = host
        self._library_name = library
        self.connect()
        self.update_info()

    def connect(self):
        try:
            self._conn = Arctic(self._host)
            self._library = self._conn[self._library_name]
        except Exception as e:
            raise e
        return 1
    def update_info(self):
        _dict = {}
        all_libs = self._conn.list_libraries()
        for lib in all_libs:
            _dict[lib] = self._conn[lib].list_symbols()
        self.data_dict = _dict

    # -----表管理------
    def check_library_exist(self, lib):
        return lib in self.data_dict

    def create_library(self, new_lib, lib_type='chunk_store', if_exist='nothing'):
        if self.check_library_exist(new_lib):
            if if_exist == 'nothing':
                return
            elif if_exist == 'raise':
                raise KeyError("{lib} already exists!".format(lib=new_lib))
            else:
                self._conn.delete_library(new_lib)
                self._conn.initialize_library(new_lib, lib_type=mongoDB.LIBRARY_TYPE[lib_type])
        else:
            self._conn.initialize_library(new_lib, lib_type=mongoDB.LIBRARY_TYPE[lib_type])
        self.update_info()
        return 1

    def delete_library(self,lib):
        if not self.check_library_exist(lib):
            return 1
        else:
            self._conn.delete_library(lib)
            self.update_info()
            return 1

    def rename_library(self,oldname,newname):
        if not self.check_library_exist(oldname):
            raise KeyError("{lib} does not exist!".format(lib=oldname))
        else:
            self._conn.rename_library(oldname, newname)
            self.update_info()
        return 

    def set_library(self, lib):
        self._library_name = lib
        self._library = self._conn[lib]

    # -----字段管理-------
    def check_symbol_exist(self,symbol):
        return symbol in self.data_dict[self._library_name]
    
    def get_date_range(self, symbol):
        data_range = self._library.get_chunk_ranges(symbol)
        start_date = next(data_range)[0].decode()
        data_range = self._library.get_chunk_ranges(symbol, reverse=True)
        end_date = next(data_range)[0].decode()
        return (start_date, end_date)

    # -----数据管理-------
    def loadFactor(self, symbol, Date=None, IDs=None):
        if not self.check_symbol_exist(symbol):
            raise KeyError("{symbol} not found in {library}".format(
                symbol=symbol, library=self._library_name))

        if Date is None:
            data = self._library.read(symbol)
        else:
            if isinstance(Date,str):
                Date = [Date]
            Date = ensure_datetime_index(Date)
            # 按时间循环提取数据
            # 数据提取方式可以分为两类
            #   第一类是每月末取一次数据，该方式同常见于去某一个因子的截面数据，特点是
            # 日期数量不多，并且不连续，以循环的方式提取效率较好
            #   第二类是每天去一次，通常见于提取指数的收盘价等行情数据的时候，此时一次
            #全部提取的效率较高
            if (len(Date) <= 2) or (len(Date) > 140):
                data = self._library.read(symbol,chunk_range=Date)
            elif (len(Date) <= 140) and (abs((Date[1]-Date[0]).days)) > 20:
                data = []
                for idate in Date:
                    idata = self._library.read(symbol,chunk_range=pd.DatetimeIndex([idate]))
                    data.append(idata)
                data = pd.concat(data)
            else:
                data = self._library.read(symbol,chunk_range=Date)
            if data.empty:
                return data
        if IDs:
            if not isinstance(IDs, list):
                IDs = [IDs]
            return data.reindex(IDs,level=1)
        else:
            return data

    def loadFactorsCrossDBs(self,symbols,IDs=None,Date=None):
        orientions = self.symbol_oriention(symbols)
        results = []
        if orientions:
            for lib in orientions:
                self.set_library(lib)
                for symbol in orientions[lib]:
                    data = self.loadFactor(symbol,Date=Date,IDs=IDs)
                    results.append(data)
        return pd.concat(results, axis=1)

    def loadFactors(self,symbols=None,IDs=None,Date=None):
        if symbols is None:
            symbols = self.data_dict[self._library]
        if not isinstance(symbols,list):
            symbols = [symbols]
        results = []
        for _s in symbols:
            data = self.loadFactor(_s,Date=Date,IDs=IDs)
            results.append(data)
        return pd.concat(results,axis=1)

    def saveFactor(self, df, library, chunk_size='D'):
        """存储因子
        df
        -----
        DataFrame(index=[Date,IDs])
        """
        self.set_library(library)
        if df.index.nlevels >= 2:
            df.reset_index(level=1, inplace=True)
            df.index = ensure_datetime_index(df.index)
            df.set_index('IDs',append=True, inplace=True)
            df.index.names = ['date','IDs']
            
            all_columns = df.columns
            for column in all_columns:
                if not self.check_symbol_exist(column):
                    self._library.write(column, df[[column]], chunk_size=chunk_size)
                    continue
                origin_items = self.loadFactor(column, Date=df.index.levels[0])
                if origin_items.empty:
                    self._library.append(column,df[[column]])
                else:
                    origin_items.update(df[[column]])
                    self._library.update(
                    column, origin_items, chunk_range=origin_items.index.levels[0])
                    
                    new = df[~df.index.isin(origin_items.index)][[column]]
                    if not new.empty:
                      self._library.append(column,new[[column]])

        elif isinstance(df.index, pd.DatetimeIndex):
            df.index.name = 'date'
            
            all_columns = df.columns
            for column in all_columns:
                if not self.check_symbol_exist(column):
                    self._library.write(column, df[[column]], chunk_size=chunk_size)
                    continue
                origin_items = self.loadFactor(column, Date=df.index)
                if origin_items.empty:
                    self._library.append(column,df[[column]])
                else:
                    origin_items.update(df[[column]])
                    self._library.update(
                    column, origin_items, chunk_range=origin_items.index)
                    
                    new = df[~df.index.isin(origin_items.index)][[column]]
                    if not new.empty:
                      self._library.append(column,new[[column]])            
        self.update_info()
        return 1

    def symbol_oriention(self,symbols):
        results = {}
        if not isinstance(symbols,list):
            symbols = [symbols]
        for lib in self.data_dict:
            _ = list(set(self.data_dict[lib]).intersection(set(symbols)))
            if _:
                results[lib] = _
        return results

def ensure_datetime_index(Date):
    if isinstance(Date,pd.DatetimeIndex):
        return Date
    else:
        if len(Date) == 1:
            Date = pd.DatetimeIndex(Date * 2)
        else:
            Date = pd.DatetimeIndex(Date)
        return Date







