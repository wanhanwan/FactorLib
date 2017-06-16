"""动量类因子"""

def return_60d(start, end, **kwargs):
    all_dates = kwargs['data_source'].trade_calendar.get_trade_days(start, end)
    universe = kwargs['data_source'].sector.get_history_ashare(all_dates)
    ids = universe.index.levels[1].unique()
    ret_60d = kwargs['data_source'].get_past_ndays_return(list(ids), 60, start, end)
    kwargs['data_source'].h5DB.save_factor(ret_60d, '/stock_momentum/')

def return_5d(start, end, **kwargs):
    all_dates = kwargs['data_source'].trade_calendar.get_trade_days(start, end)
    universe = kwargs['data_source'].sector.get_history_ashare(all_dates)
    ids = universe.index.levels[1].unique()
    ret_5d = kwargs['data_source'].get_past_ndays_return(list(ids), 5, start, end)
    kwargs['data_source'].h5DB.save_factor(ret_5d, '/stock_momentum/')

def return_20d(start, end, **kwargs):
    all_dates = kwargs['data_source'].trade_calendar.get_trade_days(start, end)
    universe = kwargs['data_source'].sector.get_history_ashare(all_dates)
    ids = universe.index.levels[1].unique()
    ret_20d = kwargs['data_source'].get_past_ndays_return(list(ids), 20, start, end)
    kwargs['data_source'].h5DB.save_factor(ret_20d, '/stock_momentum/')    

MomentumFuncListDaily = [return_5d, return_20d, return_60d]
MomentumFuncListMonthly = []