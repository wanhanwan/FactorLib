def drop_untradable_stocks(factor, **kwargs):
    """去掉不可以交易的股票"""
    env = kwargs['env']
    all_dates = factor.data.index.get_level_values(0).unique().tolist()
    stocks_trade_status = env._data_source.get_stock_trade_status(dates=all_dates)
    common = factor.data.merge(stocks_trade_status[['no_trading']],
                               left_index=True,right_index=True,how='left')
    new_data = common[common['no_trading']!=1][[factor.name]]
    factor.data = new_data

FuncList = {'drop_untradable_stocks': drop_untradable_stocks}