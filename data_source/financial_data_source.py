"""财务因子类
注:
   由于2007年修改了会计准则，导致大部分上市公司在2008年4月份更正其2006年的年报，
而Wind为了数据的连续性，会把更正后的数据直接覆盖原有数据。因此在程序设计时，我们
也同样直接使用更正后的2006年年报数据。
"""
import pandas as pd
from utils.datetime_func import DateStr2Datetime
from utils.tool_funcs import RollBackNPeriod

class FinancialDataSource(object):
    def __init__(self):
        self.data_path = None
        self.report_data = None
    
    def set_data_path(self, path):
        self.data_path = path
        self.report_data = pd.read_csv(
            path, header=0, index_col=0, converters={'actual_ann_dt': str, 'report_period': str})
    
    #最近年报因子
    def last_year(self, factor_list, date_seq):
        if not isinstance(factor_list, list):
            factor_list = [factor_list]
        try:
            wind_data = self.report_data.query("season==4")[
                ['s_info_windcode', 'actual_ann_dt', 'report_period']+factor_list]
            wind_data = wind_data[wind_data['s_info_windcode'].str.contains(r'^[0-9]')].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt']
            )
            wind_data['s_info_windcode'] = wind_data['s_info_windcode'].str[:6]
        except:
            return pd.DataFrame()
        report_ann_date = wind_data[
            ['s_info_windcode', 'report_period', 'actual_ann_dt']].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt'])
        
        _l = []
        for date in date_seq:
            last_annual_report_data = wind_data.query("actual_ann_dt <= @date | report_period=='20061231'"
                ).drop_duplicates(subset=['s_info_windcode', 'report_period'], keep='last').set_index(
                    ['s_info_windcode', 'report_period'])
            max_report_ann_dt = report_ann_date[report_ann_date.actual_ann_dt<=date].groupby('s_info_windcode').last()
            if date >= '20070430':
                max_report_ann_dt.loc[max_report_ann_dt['report_period']<'20061231', 'report_period'] = '20061231' 
            idx = max_report_ann_dt.set_index(['report_period'], append=True).index
            data = last_annual_report_data.loc[idx, factor_list]
            new_idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], data.index.get_level_values(0)])
            data.index = new_idx
            _l.append(data)
        rslt = pd.concat(_l).sort_index()
        rslt.index.names = ['date', 'IDs']
        return rslt
    
    # 最近的一期财报(包括年报和季度报告)
    def last_quater(self, factor_list, date_seq):
        if not isinstance(factor_list, list):
            factor_list = [factor_list]        
        try:
            wind_data = self.report_data[['s_info_windcode', 'actual_ann_dt', 'report_period']+factor_list]
            wind_data = wind_data[wind_data['s_info_windcode'].str.contains(r'^[0-9]')].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt']  
            )
            wind_data['s_info_windcode'] = wind_data['s_info_windcode'].str[:6]
        except:
            return pd.DataFrame()        
        report_ann_date = wind_data[
            ['s_info_windcode', 'report_period', 'actual_ann_dt']].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt'])
        
        _l = []
        for date in date_seq:
            last_annual_report_data = wind_data.query("actual_ann_dt <= @date | report_period=='20061231'"
                ).drop_duplicates(subset=['s_info_windcode', 'report_period'], keep='last').set_index(
                    ['s_info_windcode', 'report_period'])
            max_report_ann_dt = report_ann_date[report_ann_date.actual_ann_dt<=date].groupby('s_info_windcode').last()
            if date >= '20070430':
                max_report_ann_dt.loc[max_report_ann_dt['report_period']<'20061231', 'report_period'] = '20061231'            
            idx = max_report_ann_dt.set_index(['report_period'], append=True).index
            data = last_annual_report_data.loc[idx, factor_list]
            new_idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], data.index.get_level_values(0)])
            data.index = new_idx
            _l.append(data)
        rslt = pd.concat(_l).sort_index()
        rslt.index.names = ['date', 'IDs']
        return rslt        
    
    # TTM数据
    def last_ttm(self, factor_list, date_seq):
        if not isinstance(factor_list, list):
            factor_list = [factor_list]        
        try:
            wind_data = self.report_data[['s_info_windcode', 'actual_ann_dt', 'report_period']+factor_list]
            wind_data = wind_data[
                wind_data['s_info_windcode'].str.contains(r'^[0-9]')].sort_values(
                    ['s_info_windcode', 'report_period', 'actual_ann_dt'])
            wind_data['s_info_windcode'] = wind_data['s_info_windcode'].str[:6]
        except:
            return pd.DataFrame()        
        report_ann_date = wind_data[
            ['s_info_windcode', 'report_period', 'actual_ann_dt']].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt'])
        
        _l = []
        for date in date_seq:
            last_annual_report_data = wind_data.query("actual_ann_dt <= @date | report_period=='20061231'"
                ).drop_duplicates(subset=['s_info_windcode', 'report_period'], keep='last').set_index(
                    ['s_info_windcode', 'report_period'])
            
            max_report_ann_dt = report_ann_date[report_ann_date.actual_ann_dt<=date].groupby('s_info_windcode').last()  # 最近一期财报
            if date >= '20070430':
                max_report_ann_dt.loc[max_report_ann_dt['report_period']<'20061231', 'report_period'] = '20061231'                
            idx_max_report_ann_dt = max_report_ann_dt.set_index(['report_period'], append=True).index
            
            report_year = max_report_ann_dt['report_period'].str[:4]                                                    # 最近一期财报的年份
            report_season = max_report_ann_dt['report_period'].str[4:]                                                  # 最近一期财报的季度
            
            last_annual_report_dt = ((report_year.astype('int') - 1) * 10000 + 1231).astype('str')                      # 上一年度年报
            idx_last_annual_report_dt = last_annual_report_dt.to_frame().set_index('report_period', append=True).index
            
            last_year_report_dt = ((report_year.astype('int') - 1)* 10000 + report_season.astype('int')).astype('str')  # 去年同期财报
            idx_last_year_report_dt = last_year_report_dt.to_frame().set_index('report_period', append=True).index
            
            data = last_annual_report_data.loc[idx_max_report_ann_dt, factor_list].reset_index(level=1, drop=True) + \
                last_annual_report_data.loc[idx_last_annual_report_dt, factor_list].reset_index(level=1, drop=True) - \
                last_annual_report_data.loc[idx_last_year_report_dt, factor_list].reset_index(level=1, drop=True)
            
            new_idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], data.index], names=['date', 'IDs'])
            data.index = new_idx
            _l.append(data)
        rslt = pd.concat(_l).sort_index()
        return rslt
    
    # 回溯N年前同期数据
    def last_quater_back_nyear(self, factor_list, n_year, date_seq, anual_report_only=False):
        if not isinstance(factor_list, list):
            factor_list = [factor_list]        
        try:
            wind_data = self.report_data[
                ['s_info_windcode', 'year', 'season', 'actual_ann_dt']+factor_list].sort_values(
                    ['s_info_windcode', 'year', 'season', 'actual_ann_dt'])
            wind_data = wind_data[wind_data['s_info_windcode'].str.contains(r'^[0-9]')]
            wind_data['s_info_windcode'] = wind_data['s_info_windcode'].str[:6]
            if anual_report_only:
                wind_data = wind_data[wind_data['season']==4]
        except:
            return pd.DataFrame()
        report_ann_date = wind_data[
            ['s_info_windcode', 'year', 'season', 'actual_ann_dt']]
        
        _l = []
        for date in date_seq:
            last_annual_report_data = wind_data.query(
                "actual_ann_dt <= @date | (year==2006 & season==4)").drop_duplicates(
                    subset=['s_info_windcode', 'year', 'season'], keep='last').set_index(
                        ['s_info_windcode', 'year', 'season'])
            max_report_ann_dt = report_ann_date[report_ann_date.actual_ann_dt<=date].groupby('s_info_windcode').last()  # 最近一期财报
            if date >= '20070430':
                max_report_ann_dt.loc[max_report_ann_dt['report_period']<'20061231', 'report_period'] = '20061231'                
            last_nyear = max_report_ann_dt['year'] - n_year
            idx_last_nyear = pd.MultiIndex.from_arrays([max_report_ann_dt.index, last_nyear, max_report_ann_dt['season']],
                                                                names=['s_info_windcode', 'year', 'season'])
            data = last_annual_report_data.loc[idx_last_nyear, factor_list].reset_index(level=[1, 2], drop=True)
            new_idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], data.index], names=['date', 'IDs'])
            data.index = new_idx
            _l.append(data)            
        return pd.concat(_l).sort_index()
    
    # 回溯N期之前TTM数据
    def last_ttm_back_nperiod(self, factor_list, n_period, date_seq):
        if not isinstance(factor_list, list):
            factor_list = [factor_list]        
        try:
            wind_data = self.report_data[['s_info_windcode', 'actual_ann_dt', 'report_period']+factor_list]
            wind_data = wind_data[
                wind_data['s_info_windcode'].str.contains(r'^[0-9]')].sort_values(
                    ['s_info_windcode', 'report_period', 'actual_ann_dt'])
            wind_data['s_info_windcode'] = wind_data['s_info_windcode'].str[:6]
        except:
            return pd.DataFrame()        
        report_ann_date = wind_data[
            ['s_info_windcode', 'report_period', 'actual_ann_dt']].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt'])
        
        _l = []
        for date in date_seq:
            last_annual_report_data = wind_data.query("actual_ann_dt <= @date | report_period=='20061231'"
                ).drop_duplicates(subset=['s_info_windcode', 'report_period'], keep='last').set_index(
                    ['s_info_windcode', 'report_period'])
            
            max_report_ann_dt = report_ann_date[report_ann_date.actual_ann_dt<=date].groupby('s_info_windcode').last()  # 最近一期财报
            if date >= '20070430':
                max_report_ann_dt.loc[max_report_ann_dt['report_period']<'20061231', 'report_period'] = '20061231'                
            object_report_period = max_report_ann_dt['report_period'].apply(RollBackNPeriod, n_period=n_period).to_frame()
            idx_object_report_period = object_report_period.set_index('report_period', append=True).index
            object_report_data = last_annual_report_data.loc[idx_object_report_period, factor_list].reset_index(level=1, drop=True)
            
            object_report_year = object_report_period['report_period'].str[:4]
            last_annual_report_dt = ((object_report_year.astype('int') - 1) * 10000 + 1231).astype('str')                      # 上一年度年报
            idx_last_annual_report_dt = last_annual_report_dt.to_frame().set_index('report_period', append=True).index            
            
            object_report_season = object_report_period['report_period'].str[4:]
            last_year_report_dt = ((object_report_year.astype('int') - 1)* 10000 + object_report_season.astype('int')).astype('str')  # 去年同期财报
            idx_last_year_report_dt = last_year_report_dt.to_frame().set_index('report_period', append=True).index
            
            data = object_report_data + \
                last_annual_report_data.loc[idx_last_annual_report_dt, factor_list].reset_index(level=1, drop=True) - \
                last_annual_report_data.loc[idx_last_year_report_dt, factor_list].reset_index(level=1, drop=True)
            data.update(object_report_data[object_report_season=='1231'])
            
            new_idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], data.index], names=['date', 'IDs'])
            data.index = new_idx
            _l.append(data)
        rslt = pd.concat(_l).sort_index()            
        return rslt


class BalanceSheet(FinancialDataSource):
    def __init__ (self, data_path="D:/data/h5/balancesheet.csv"):
        super(BalanceSheet, self).__init__()
        self.set_data_path(data_path)


class IncomeSheet(FinancialDataSource):
    def __init__(self, data_path="D:/data/h5/incomesheet.csv"):
        """Constructor"""
        super(IncomeSheet, self).__init__()
        self.set_data_path(data_path)
        
    # 单季度数据
    def last_single_quater(self, factor_list, date_seq):
        if not isinstance(factor_list, list):
            factor_list = [factor_list]        
        try:
            wind_data = self.report_data[
                ['s_info_windcode', 'report_period', 'actual_ann_dt']+factor_list].sort_values(
                    ['s_info_windcode', 'report_period', 'actual_ann_dt'])
            wind_data = wind_data[wind_data['s_info_windcode'].str.contains(r'^[0-9]')]
            wind_data['s_info_windcode'] = wind_data['s_info_windcode'].str[:6]
        except:
            return pd.DataFrame()
        report_ann_date = wind_data[
            ['s_info_windcode', 'report_period', 'actual_ann_dt']].sort_values(
                ['s_info_windcode', 'report_period', 'actual_ann_dt'])        
        
        _l = []
        for date in date_seq:
            last_annual_report_data = wind_data.query("actual_ann_dt <= @date | report_period=='20061231'"
                ).drop_duplicates(subset=['s_info_windcode', 'report_period'], keep='last').set_index(
                    ['s_info_windcode', 'report_period'])
            
            max_report_ann_dt = report_ann_date[report_ann_date.actual_ann_dt<=date].groupby('s_info_windcode').last()  # 最近一期财报
            if date >= '20070430':
                max_report_ann_dt.loc[max_report_ann_dt['report_period']<'20061231', 'report_period'] = '20061231'                
            idx_max_report_ann_dt = max_report_ann_dt.set_index(['report_period'], append=True).index
            
            last_season = max_report_ann_dt['report_period'].apply(RollBackNPeriod, n_period=1)
            idx_last_season = pd.MultiIndex.from_arrays([max_report_ann_dt.index, last_season], names=['s_info_windcode', 'report_period'])
            last_season_data = last_annual_report_data.loc[idx_last_season, factor_list].copy()
            last_season_data.loc[last_season.str.contains(r'1231$'), factor_list] = 0
            
            data = last_annual_report_data.loc[idx_max_report_ann_dt, factor_list].reset_index(level=1, drop=True) - \
                last_season_data.reset_index(level=1, drop=True)
            
            new_idx = pd.MultiIndex.from_product([[DateStr2Datetime(date)], data.index], names=['date', 'IDs'])
            data.index = new_idx
            _l.append(data)
        return pd.concat(_l).sort_index()

if __name__=='__main__':
    from data_source import tc, h5
    all_dates = tc.get_trade_days('20070101', '20170502')
    inc = IncomeSheet()
    data = inc.last_year(['net_profit_excl_min_int_inc'], all_dates)
    data.columns = ['net_profit_last_year']
    h5.save_factor(data, '/stock_financial_data/')