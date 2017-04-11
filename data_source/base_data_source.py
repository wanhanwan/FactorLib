import pandas as pd

from const import SW_INDUSTRY_DICT,CS_INDUSTRY_DICT,MARKET_INDEX_DICT
from utils.tool_funcs import parse_industry

class base_data_source(object):
    def __init__(self, sector):
        self.mongoDB = sector.mongoDB
        self.trade_calendar = sector.trade_calendar
        self.sector = sector

    def load_factor(self, symbol, ids=None, dates=None, start_date=None, end_date=None, others=None):
        if not isinstance(symbol,list):
            symbol = [symbol]
        dates = self.trade_calendar.get_trade_days(start_date,end_date) if dates is None else dates
        return self.mongoDB.loadFactorsCrossDBs(symbol,IDs=ids,Date=dates)

    def get_history_price(self, ids, dates=None, start_date=None, end_date=None,
                          freq='1d', type='stock', adjust=False):
        """ 提取股票收盘价
        adjust:是否复权
        """
        if freq != '1d':
            raise ValueError("no data with frequency of %s, only support 1d now"%freq)
        if type == 'stock':
            database = 'stocks'
            symbol = 'adj_close' if adjust else 'close'
        else:
            database = 'indexprices'
            symbol = 'close'

        self.mongoDB.set_library(database)
        if dates is not None:
            return self.mongoDB.loadFactors(symbols=[symbol],Date=dates, IDs=ids)
        elif (start_date is not None) or (end_date is not None):
            dates = self.trade_calendar.get_trade_days(start_date, end_date)
            return  self.mongoDB.loadFactors([symbol],Date=dates,IDs=ids)

    def get_period_return(self, ids, start_date, end_date, type='stock'):
        """计算证券的区间收益
        区间收益 = 开始日收盘价 / 终止日收盘价 - 1
        返回: Series(index=IDs)
        """
        prices = self.get_history_price(ids,dates=[start_date,end_date],
                                        type=type, adjust=True)
        prices = prices.swaplevel().sort_index().unstack()
        _cum_return_fun = lambda x:x.iloc[1] / x.iloc[0] -1
        period_return = prices.apply(_cum_return_fun, axis=1)
        period_return.name = 'returns'
        return period_return.reindex(ids)

    def get_fix_period_return(self, ids, freq, start_date, end_date, type='stock'):
        """对证券的日收益率序列进行resample
        在开始日与结束日之间计算固定频率的收益率
        """
        dates = self.trade_calendar.get_trade_days(start_date=start_date,end_date=end_date,freq=freq)
        if (start_date not in dates) and (start_date is not None) and (self.trade_calendar.is_trade_day(start_date)):
            dates.append(start_date)
        if (end_date not in dates) and (end_date is not None) and (self.trade_calendar.is_trade_day(start_date)):
            dates.append(end_date)
        dates.sort()
        prices = self.get_history_price(ids,dates=dates,type=type,adjust=True)
        prices =prices.swaplevel().sort_index().unstack()
        _cum_return_fun = lambda x: x[1] / x[0] - 1
        returns = prices.rolling(window=2,axis=1,min_periods=2).apply(_cum_return_fun)
        returns.columns = pd.MultiIndex.from_product([['returns'],pd.DatetimeIndex(dates)])
        returns.columns.names = ['returns', 'date']
        return returns.stack().swaplevel()

    def get_stock_trade_status(self, ids=None, dates=None, start_date=None, end_date=None,freq='1d'):
        """获得股票交易状态信息,包括停牌、ST、涨跌停"""
        if dates is None:
            dates = self.trade_calendar.get_trade_days(start_date=start_date,end_date=end_date,freq=freq)
        elif not isinstance(dates,list):
            dates = [dates]
        if (start_date not in dates) and (start_date is not None):
            dates.append(start_date)
        if (end_date not in dates) and (end_date is not None):
            dates.append(end_date)
        dates.sort()

        st = self.sector.get_st(dates)
        suspend = self.sector.get_suspend(dates)
        uplimit = self.sector.get_uplimit(dates)
        downlimit = self.sector.get_downlimit(dates)

        trade_status = pd.concat([st,suspend,uplimit,downlimit],axis=1)
        trade_status = trade_status.where(pd.isnull(trade_status), other=1)
        trade_status.fillna(0,inplace=True)
        trade_status.columns = ['st','suspend','uplimit','downlimit']
        trade_status['no_trading'] = trade_status.any(axis=1)
        if not ids:
            return trade_status
        else:
            if not isinstance(ids,list):
                ids = [ids]
            idx = pd.MultiIndex.from_product([ids,dates])
            idx.names = ['Date','IDs']
            return trade_status.reindex(idx,fill_value=0)

class sector(object):
    def __init__(self, mongoDB, trade_calendar):
        self.mongoDB = mongoDB
        self.trade_calendar = trade_calendar

    def get_st(self, dates=None, start_date=None, end_date=None):
        """某一个时间段内st的股票"""
        dates = self.trade_calendar.get_trade_days(start_date,end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        self.mongoDB.set_library("stocks")
        st_list = self.mongoDB.loadFactors(symbols=['is_st'],Date=dates)
        st_list = st_list.query('is_st == 1')
        return st_list['is_st']

    def get_suspend(self, dates=None, start_date=None, end_date=None):
        """某一时间段停牌的股票"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        self.mongoDB.set_library('stocks')
        suspend = self.mongoDB.loadFactors(symbols=['volume'],Date=dates)
        suspend = suspend.query('volume == 0')
        return  suspend['volume']

    def get_uplimit(self, dates=None, start_date=None, end_date=None):
        """某一时间段内涨停的股票"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        self.mongoDB.set_library('stocks')
        uplimit = self.mongoDB.loadFactors(symbols=['high','low','daily_returns_%'], Date=dates)
        uplimit = uplimit[(uplimit['high']==uplimit['low'])&(uplimit['daily_returns_%']>9.5)]
        return uplimit['high']

    def get_downlimit(self,dates=None, start_date=None, end_date=None):
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        self.mongoDB.set_library('stocks')
        downlimit = self.mongoDB.loadFactors(symbols=['high','low','daily_returns_%'], Date=dates)
        downlimit = downlimit[(downlimit['high']==downlimit['low'])&(downlimit['daily_returns_%']<-9.5)]
        return downlimit['high']

    def get_index_members(self, ids, dates=None, start_date=None, end_date=None):
        """某一个时间段内指数成分股,可以是市场指数也可以是行业指数
        目前,市场指数包括:万得全A(880011)、上证50(000016)、中证500(000905)、中证800(000906)、创业板综(399102)和
        沪深300(000300)。
        行业指数包括:中信一级行业和申万一级行业
        """
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        if ids in MARKET_INDEX_DICT:
            symbol = ids
            other = "'%s' = 1"%ids
        elif ids in SW_INDUSTRY_DICT:
            symbol = 'sw_level_1'
            other = 'sw_level_1 = %s'%SW_INDUSTRY_DICT[ids]
        else:
            symbol = 'cs_level_1'
            other = 'cs_level_1 = %s'%CS_INDUSTRY_DICT[ids]
        self.mongoDB.set_library("indexes")
        index_members = self.mongoDB.loadFactors(symbols=[symbol],Date=dates)
        return  index_members

    def get_stock_industry_info(self, ids, industry='中信一级', start_date=None, end_date=None, dates=None):
        """股票行业信息"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        symbol = parse_industry(industry)
        self.mongoDB.set_library('indexes')
        industry_info = self.mongoDB.loadFactors(symbols=[symbol],IDs=ids,Date=dates)
        return industry_info
    
    def get_index_weight(self, ids, start_date=None, end_date=None, dates=None):
        """获取指数个股权重"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        symbol = '_{id}_weight'.format(id=ids)
        
        self.mongoDB.set_library('indexes')
        weight = self.mongoDB.loadFactor(symbol, Date=dates).sort_index()
        weight = weight.unstack().reindex(pd.DatetimeIndex(dates), method='ffill').stack()
        return weight
    
    def get_index_industry_weight(self, ids, industry_name='中信一级', start_date=None,
                                  end_date=None, dates=None):
        """获取指数的行业权重"""
        symbol = parse_industry(industry_name)
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        index_weight = self.get_index_weight(ids, dates=dates)
        all_stocks = list(index_weight.index.levels[1].unique())
        industry_data = self.get_stock_industry_info(all_stocks, industry=industry_name, dates=dates)
        common = pd.merge(index_weight, industry_data, left_index=True, right_index=True, how='left')
        index_industry_weight = common.reset_index().groupby(['date', symbol])['weight'].sum()
        return index_industry_weight

    def get_history_ashare(self, date):
        """获得某一天的所有上市A股"""
        if not isinstance(date,list):
            date = [date]
        self.mongoDB.set_library('stocks')
        stocks = self.mongoDB.loadFactors(symbols=['list_date','delist_date'])
        stock_list = []
        for idate in date:
            _d = stocks.query("list_date < '%s' & delist_date > '%s'"%(idate,idate))
            _d.index = _d.index.set_levels(levels=pd.DatetimeIndex([idate]),level=0)
            stock_list.append(_d[['list_date']])
        return pd.concat(stock_list)

    def get_stock_info(self, ids, date):
        """获得上市公司的信息。
        信息包括公司代码、公司简称、上市日期、所属行业(中信一级和申万一级)以及主要指数"""
        if not isinstance(date,list):
            date = [date]
        if not isinstance(ids, list):
            ids = [ids]
        self.mongoDB.set_library('stocks')
        stock_name_listdate = self.mongoDB.loadFactors(symbols=['Name','list_date'], IDs=ids)
        stock_name_listdate = stock_name_listdate.reset_index(level=0,drop=True)

        self.mongoDB.set_library('indexes')
        stock_members = self.mongoDB.loadFactors(IDs=ids, Date=date).swaplevel()
        stock_info = pd.merge(stock_members,stock_name_listdate,left_index=True,right_index=True,how='left')
        stock_info = stock_info.swaplevel()
        return stock_info
