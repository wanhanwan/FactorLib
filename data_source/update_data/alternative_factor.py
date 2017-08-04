"""其他因子"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from QuantLib.tools import df_rolling


# 特异度
def iffr(start, end, **kwargs):
    startdate = kwargs['data_source'].trade_calendar.tradeDayOffset(start, -30)
    all_dates = kwargs['data_source'].trade_calendar.get_trade_days(startdate, end)
    daily_ret = kwargs['data_source'].load_factor('daily_returns', '/stocks/', dates=all_dates)
    factors = {'/time_series_factors/': ['rf', 'mkt_rf', 'smb', 'hml']}
    three_factors = kwargs['data_source'].h5DB.load_factors(factors, dates=all_dates).reset_index(level=1, drop=True)
    
    daily_ret = daily_ret['daily_returns'].unstack()
    data = pd.concat([daily_ret, three_factors], axis=1)
    
    # 计算特质收益率因子

    r_square = df_rolling(data, 20, _calRSquareApplyFunc)

    # 存储数据到数据库
    r_square = r_square.stack()
    r_square.index.names =['date','IDs']
    r_square = r_square.to_frame('iffr')
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
    y = y - x[:, 0]
    x = x[:, 1:]
    ols = sm.OLS(y, x, missing='drop').fit()
    r_square = ols.rsquared
    return r_square


def _calRSquareApplyFunc(data_frame):
    r_square = np.apply_along_axis(_calRSquare, 0, data_frame[:, :-4], data_frame[:, -4:])
    return r_square

AlternativeFuncListMonthly = []
AlternativeFuncListDaily = [iffr]

if __name__ == '__main__':
    from data_source import data_source
    iffr('20170701', '20170731', data_source=data_source)