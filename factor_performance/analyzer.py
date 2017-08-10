import pandas as pd
import pyfolio as pf
import os
from empyrical import stats
from data_source import data_source
from utils.datetime_func import (GetDatetimeLastDayOfMonth,
                                 GetDatetimeLastDayOfYear,
                                 GetDatetimeLastDayOfWeek)


class Analyzer(object):
    def __init__(self, pkl_path, benchmark_name):
        self.rootdir = os.path.dirname(pkl_path)
        self.table = pd.read_pickle(pkl_path)
        self.benchmark_return = self._return_of_benchmark(benchmark_name)
        self.portfolio_return = self._return_of_portfolio
        self.active_return = stats._adjust_returns(self.portfolio_return, self.benchmark_return)

    @property
    def _return_of_portfolio(self):
        unit_net_value = self.table['portfolio']['unit_net_value']
        return (unit_net_value / unit_net_value.shift(1).fillna(1) - 1).rename('portfolio_return')

    def _return_of_benchmark(self, name):
        ret = data_source.load_factor('daily_returns_%', '/indexprices/', ids=[self.benchmark_return.name])
        return ret.reset_index(level=1, drop=True)['daily_returns_%'].rename(name)

    @property
    def abs_annual_return(self):
        return stats.annual_return(self.portfolio_return)

    @property
    def rel_annual_return(self):
        return stats.annual_return(self.active_return)

    @property
    def abs_annual_volatility(self):
        return stats.annual_volatility(self.portfolio_return)

    @property
    def rel_annual_volatility(self):
        return stats.annual_volatility(self.active_return)

    @property
    def abs_sharp_ratio(self):
        return stats.sharpe_ratio(self.portfolio_return, simple_interest=True)

    @property
    def rel_sharp_ratio(self):
        return stats.sharpe_ratio(self.active_return, simple_interest=True)

    @property
    def abs_maxdrawndown(self):
        return stats.max_drawdown(self.portfolio_return)

    @property
    def rel_maxdrawdown(self):
        return stats.max_drawdown(self.active_return)

    @property
    def abs_weekly_performance(self):
        idx = GetDatetimeLastDayOfWeek(self.portfolio_return.index)
        gfunc = [lambda x:x.year, lambda x:x.isocalendar()[1]]
        df = self.portfolio_return.groupby(gfunc).agg(stats.cum_returns).rename(columns=['weekly_return'])
        df.index = idx
        return df

    @property
    def rel_weekly_performance(self):
        idx = GetDatetimeLastDayOfWeek(self.benchmark_return.index)
        gfunc = [lambda x:x.year, lambda x:x.isocalendar()[1]]
        df = self.benchmark_return.groupby(gfunc).agg(stats.cum_returns).rename(columns=['weekly_return'])
        df.index = idx
        return self.abs_weekly_performance - df

    @property
    def abs_monthly_performance(self):
        idx = GetDatetimeLastDayOfMonth(self.portfolio_return)
        gfunc = [lambda x: x.year, lambda x: x.month]
        df = self.portfolio_return.groupby(gfunc).agg([])

    def range_pct(self, start, end, rel=True):
        if rel:
            return stats.cum_returns(self.active_return.loc[start:end])
        else:
            return stats.cum_returns(self.portfolio_return.loc[start:end])



