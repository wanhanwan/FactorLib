"""其他因子"""

import pandas as pd
import numpy as np
import statsmodels.api as sm


# 特异度
def iffr(start, end, **kwargs):
    all_dates = kwargs['data_source'].trade_calendar.get_trade_days(start, end)
    daily_ret = kwargs['data_source'].load_factor('daily_returns', '/stocks/', dates=all_dates)
    factors = {'/time_series_factors/': ['rf', 'mkt_rf', 'smb', 'hml']}
    three_factors = kwargs['data_source'].h5DB.load_factors(factors, dates=all_dates).reset_index(level=1, drop=True)
    
    daily_ret = daily_ret['daily_returns'].unstack()
    data = pd.concat([daily_ret, three_factors], axis=1)
    
    # %%计算特质收益率因子
    r_square = data.groupby(_dateGroupFun).apply(_calRSquareApplyFunc)
    # 把最终的因子整理成sqliteDB格式
    r_square = r_square.stack().reset_index().rename(columns={'level_0': 'date',
                                                              'level_1': 'IDs',
                                                              0: 'iffr'})
    all_monthly_dates = kwargs['data_source'].trade_calendar.get_trade_days(start, end, '1m')
    all_year_month = [x[:6] for x in all_monthly_dates]
    idx = r_square['date'].apply(lambda x: all_year_month.index(x))
    new_dates = idx.apply(lambda x:all_monthly_dates[x])
    r_square['date'] = new_dates
    
    # %% 存储数据到数据库
    r_square['date']=pd.DatetimeIndex(r_square['date'])
    r_square.set_index(['date','IDs'],inplace=True)
    kwargs['data_source'].h5DB.save_factor(r_square, '/stock_alternative/')    
    
# %%构造一个线性回归的函数，计算拟合优度
def _calRSquare(y, x):
    '''线性回归计算拟合优度
    y: Series
    x: DataFrame,第一列为无风险收益率
    '''
    data_len = len(y)
    if pd.notnull(y).sum() / data_len < 0.7:
        return np.nan
    y = (y - x.iloc[:, 0]).values
    x = x.iloc[:, 1:].values
    ols = sm.OLS(y, x, missing='drop').fit()
    r_square = ols.rsquared
    return r_square

def _calRSquareApplyFunc(data_frame):
    r_square = data_frame.iloc[:, :-4].apply(_calRSquare,
                                             args=(data_frame.iloc[:, -4:],))
    return r_square

def _dateGroupFun(x):
    return str(x.year*100+x.month)

AlternativeFuncListMonthly = [iffr]
AlternativeFuncListDaily = []