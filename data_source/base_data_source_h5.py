import pandas as pd

from const import SW_INDUSTRY_DICT,CS_INDUSTRY_DICT,MARKET_INDEX_DICT
from utils.tool_funcs import parse_industry, get_industry_names
from utils.datetime_func import DateStr2Datetime
from datetime import timedelta

class base_data_source(object):
    def __init__(self, sector):
        self.h5DB = sector.h5DB
        self.trade_calendar = sector.trade_calendar
        self.sector = sector

    def load_factor(self, symbol, factor_path, ids=None, dates=None, start_date=None, end_date=None, idx=None):
        if idx is None:
            dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        return self.h5DB.load_factor(symbol, factor_path, ids=ids, dates=dates,idx=idx)

    def get_history_price(self, ids, dates=None, start_date=None, end_date=None,
                          freq='1d', type='stock', adjust=False):
        """ 提取股票收盘价
        adjust:是否复权
        """
        if freq != '1d':
            raise ValueError("no data with frequency of %s, only support 1d now"%freq)
        if type == 'stock':
            database = '/stocks/'
            symbol = 'adj_close' if adjust else 'close'
        else:
            database = '/indexprices/'
            symbol = 'close'
        if dates is not None:
            return self.h5DB.load_factor(symbol, database, dates=dates, ids=ids)
        elif (start_date is not None) or (end_date is not None):
            dates = self.trade_calendar.get_trade_days(start_date, end_date)
            return  self.h5DB.load_factor(symbol, database, dates=dates, ids=ids)

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
        if (end_date not in dates) and (end_date is not None) and (self.trade_calendar.is_trade_day(end_date)):
            dates.append(end_date)
        dates.sort()
        prices = self.get_history_price(ids, dates=dates, type=type, adjust=True)
        prices =prices.swaplevel().sort_index().unstack()
        _cum_return_fun = lambda x: x[1] / x[0] - 1
        returns = prices.rolling(window=2,axis=1,min_periods=2).apply(_cum_return_fun)
        returns.columns = pd.MultiIndex.from_product([['returns'], pd.DatetimeIndex(dates)])
        returns.columns.names = ['returns', 'date']
        return returns.stack().swaplevel()

    def get_past_ndays_return(self, ids, window, start_date, end_date, type='stock'):
        """计算证券在过去N天的累计收益"""
        data_init_date = self.trade_calendar.tradeDayOffset(start_date, -window)
        all_dates = self.trade_calendar.get_trade_days(data_init_date, end_date)
        price = self.get_history_price(ids, all_dates, adjust=True, type=type).unstack().sort_index()
        cum_returns = (price / price.shift(window) - 1).stack()
        cum_returns.columns=['return_%dd'%window]
        return cum_returns.ix[DateStr2Datetime(start_date):DateStr2Datetime(end_date)]

    def get_periods_return(self, ids, dates, type='stock'):
        """
        计算dates序列中前后两个日期之间的收益率(start_date,end_date]
        :param ids: 股票ID序列
        :param dates: 日期序列
        :return:  stock_returns
        """
        def _cal_return(data):
            data_shift = data.shift(1)
            return data/data_shift - 1
        dates.sort()
        close_prices = self.get_history_price(ids,dates=dates,type=type).sort_index()
        ret = close_prices.groupby('IDs', group_keys=False).apply(_cal_return)
        ret.index.names = ['date', 'IDs']
        ret.columns = ['returns']
        return ret


    def get_history_bar(self, ids, start, end, adjust=False, type='stock', freq='1d'):
        """
         历史K线
        :param ids: stock ids
        :param start: start date
        :param end: end date
        :param adjust: 是否复权
        :param type: stock or index
        :param freq: frequency
        :return: high open low close avgprice volume amount turnover pctchange
        """
        from functools import partial
        dates = self.trade_calendar.get_trade_days(start, end, freq)
        daily_dates = self.trade_calendar.get_trade_days(start, end)
        if type == 'stock':
            data_dict = {'/stocks/':['high','low','close', 'volume'],
                         '/stock_liquidity/':['turn']}
            data = self.h5DB.load_factors(data_dict, ids=ids, dates=daily_dates)
        groupfunc = partial(self.trade_calendar.latest_trade_day, trade_days=pd.DatetimeIndex(dates))
        group_dates = data.index.levels[0].to_series().apply(groupfunc)
        data = data.join(group_dates.rename('group_date'))
        cal_func = {'high':'max','low':'min',
                    'close':'last','volume':'sum','turn':'sum'}
        bar = data.groupby(['IDs','group_date']).agg(cal_func).swaplevel()
        bar.index.names = ['date', 'IDs']
        return bar


    def get_stock_trade_status(self, ids=None, dates=None, start_date=None, end_date=None, freq='1d'):
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

        trade_status = self.load_factor('no_trading', '/trade_status/', dates=dates)
        if not ids:
            return trade_status
        else:
            if not isinstance(ids,list):
                ids = [ids]
            idx = pd.MultiIndex.from_product([dates, ids])
            idx.names = ['date','IDs']
            return trade_status.reindex(idx, fill_value=0)

class sector(object):
    def __init__(self, h5, trade_calendar):
        self.h5DB = h5
        self.trade_calendar = trade_calendar

    def get_st(self, dates=None, start_date=None, end_date=None):
        """某一个时间段内st的股票"""
        dates = self.trade_calendar.get_trade_days(start_date,end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        st_list = self.h5DB.load_factor('is_st', '/stocks/', dates=dates)
        st_list = st_list.query('is_st == 1')
        return st_list['is_st']

    def get_suspend(self, dates=None, start_date=None, end_date=None):
        """某一时间段停牌的股票"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        suspend = self.h5DB.load_factor('volume', '/stocks/', dates=dates)
        suspend = suspend.query('volume == 0')
        return  suspend['volume']

    def get_uplimit(self, dates=None, start_date=None, end_date=None):
        """某一时间段内涨停的股票"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        factors = {'/stocks/': ['high', 'low', 'daily_returns_%'],}
        uplimit = self.h5DB.load_factors(factors, dates=dates)
        uplimit = uplimit[(uplimit['high']==uplimit['low'])&(uplimit['daily_returns_%']>9.5)]
        return uplimit['high']

    def get_downlimit(self,dates=None, start_date=None, end_date=None):
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates,list):
            dates = [dates]
        factors = {'/stocks/': ['daily_returns_%', 'high', 'low']}
        downlimit = self.h5DB.load_factors(factors, dates=dates)
        downlimit = downlimit[(downlimit['high']==downlimit['low'])&(downlimit['daily_returns_%']<-9.5)]
        return downlimit['high']

    def get_index_members(self, ids, dates=None, start_date=None, end_date=None):
        """某一个时间段内指数成分股,可以是市场指数也可以是行业指数
        目前,市场指数包括:
             万得全A(880011)、上证50(000016)、中证500(000905)、中证800(000906)、创业板综(399102)和
        沪深300(000300)。
        行业指数包括:中信一级行业和申万一级行业
        """
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        all_stocks = self.get_history_ashare(dates)

        if isinstance(dates, str):
            dates = [dates]
        if ids == '全A':
            return all_stocks
        if ids in MARKET_INDEX_DICT:
            index_members = self.h5DB.load_factor('_%s' % ids, '/indexes/', dates=dates)
        elif ids in SW_INDUSTRY_DICT:
            temp = self.h5DB.load_factor('sw_level_1', '/indexes/', dates=dates)
            index_members = temp[temp['sw_level_1'] == int(ids)]
        else:
            temp = self.h5DB.load_factor('cs_level_1', '/indexes/', dates=dates)
            index_members = temp[temp['cs_level_1'] == int(ids[2:])]
        return  index_members[index_members.index.isin(all_stocks.index)]

    def get_stock_industry_info(self, ids, industry='中信一级', start_date=None, end_date=None, dates=None):
        """股票行业信息"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        if not isinstance(dates, list):
            dates = [dates]
        symbol = parse_industry(industry)
        industry_info = self.h5DB.load_factor(symbol, '/indexes/', ids=ids, dates=dates)
        return get_industry_names(symbol, industry_info)

    def get_index_weight(self, ids, start_date=None, end_date=None, dates=None):
        """获取指数个股权重"""
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        symbol = '_{id}_weight'.format(id=ids)

        weight = self.h5DB.load_factor(symbol, '/indexes/', dates=dates).sort_index()
        weight = weight.unstack().reindex(pd.DatetimeIndex(dates), method='ffill').stack()
        weight.index.names = ['date', 'IDs']
        return weight

    def get_index_industry_weight(self, ids, industry_name='中信一级', start_date=None,
                                  end_date=None, dates=None):
        """获取指数的行业权重"""
        symbol = parse_industry(industry_name)
        dates = self.trade_calendar.get_trade_days(start_date, end_date) if dates is None else dates
        index_weight = self.get_index_weight(ids, dates=dates)
        all_stocks = list(index_weight.index.levels[1].unique())
        industry_data = self.get_stock_industry_info(all_stocks, industry=industry_name, dates=dates)
        common = pd.concat([index_weight, industry_data], join='inner', axis=1)
        index_industry_weight = common.reset_index().groupby(['date', symbol])[index_weight.columns[0]].sum()
        return index_industry_weight

    def get_history_ashare(self, dates):
        """获得某一天的所有上市A股"""
        if isinstance(dates, str):
            dates = [dates]
        stocks = self.h5DB.load_factor('ashare', '/indexes/', datess=dates)
        return stocks

    def get_ashare_onlist(self, dates, months_filter=24):
        """获得某一天已经上市的公司，并且上市日期不少于24个月"""
        ashare = self.get_history_ashare(dates).swaplevel()
        ashare_onlist_date = self.h5DB.load_factor(
            'list_date', '/stocks/').reset_index(level=0, drop=True)
        ashare_backdoordate = self.h5DB.load_factor(
            'backdoordate', '/stocks/').reset_index(level=0, drop=True)   # 借壳上市日期
        ashare_info = pd.merge(ashare, ashare_onlist_date, left_index=True, right_index=True, how='left')
        ashare_info = ashare_info.join(ashare_backdoordate).reset_index()
        def f:
            if x['date'] >= DateStr2Datetime(x['backdoordate']):
                return x['backdoordate']
            else:
                return x['list_date']
        ashare_info['list_date'] = ashare_info.apply(f, axis=1)
        onlist_period = ashare_info['date'] - ashare_info['new_listdate'].apply(DateStr2Datetime)
        temp_ind = (onlist_period / timedelta(1)) > months_filter * 30
        return ashare_info.set_index(['date', 'IDs']).loc[temp_ind.values, ['list_date']].copy()

    def get_stock_info(self, ids, date):
        """获得上市公司的信息。
        信息包括公司代码、公司简称、上市日期、所属行业(中信一级)"""
        if not isinstance(date,list):
            date = [date]
        if (not isinstance(ids, list)) and (ids is not None):
            ids = [ids]
        factors = {'/stocks/': ['Name', 'list_date']}
        stock_name_listdate = self.h5DB.load_factors(factors, ids=ids)
        stock_name_listdate = stock_name_listdate.reset_index(level=0,drop=True)

        stock_members = self.get_stock_industry_info(ids, dates=date).swaplevel()
        stock_info = pd.merge(stock_members,stock_name_listdate,left_index=True,right_index=True,how='left')
        stock_info = stock_info.swaplevel()
        return stock_info
