from QuantLib import stockFilter
import pandas as pd

def drop_untradable_stocks(factor, **kwargs):
    """去掉不可以交易的股票"""
    env = kwargs['env']
    all_dates = factor.data.index.get_level_values(0).unique().tolist()
    stocks_trade_status = env._data_source.get_stock_trade_status(dates=all_dates)
    common = factor.data.merge(stocks_trade_status[['no_trading']],
                               left_index=True,right_index=True,how='left')
    new_data = common[common['no_trading']!=1][[factor.name]]
    factor.data = new_data


def drop_new_and_untradable(factor, **kwargs):
    """去掉上市不满6个月的新股以及涨跌停、刚复牌、恢复ST"""
    stocklist = pd.DataFrame([[1]*len(factor)], columns=['a'], index=factor.index)
    new_stocks = stockFilter._drop_latest_st(stockFilter.typical(stocklist), months=3)
    return factor.reindex(new_stocks.index)


FuncList = {'drop_untradable_stocks': drop_untradable_stocks,
            'drop_new_and_untradable': drop_new_and_untradable}