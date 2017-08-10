import pandas as pd
import numpy as np
import pyfolio as pf
import os
from empyrical import stats
from data_source import data_source
from utils.datetime_func import (GetDatetimeLastDayOfMonth,
                                 GetDatetimeLastDayOfYear,
                                 GetDatetimeLastDayOfWeek,
                                 GetDatetimeLastDay)


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
        unit_net_value.index.name = 'date'
        return (unit_net_value / unit_net_value.shift(1).fillna(1) - 1).rename('portfolio_return')

    def _return_of_benchmark(self, name):
        try:
            ret = data_source.load_factor('daily_returns_%', '/indexprices/', ids=[self.benchmark_return.name]) / 100
        except Exception as e:
            print(e)
            return pd.Series(np.zeros(len(self.portfolio_return)), index=self.portfolio_return.index, name=name)
        return ret.reset_index(level=1, drop=True)['daily_returns_%'].rename(name)

    def resample_active_return_of(self, frequence):
        idx = GetDatetimeLastDay(self.portfolio_return, freq=frequence)
        portfolio_return = self.resample_portfolio_return(frequence)
        benchmark_return = self.resample_benchmark_return(frequence)
        active_return = stats._adjust_returns(portfolio_return, benchmark_return)
        active_return.index = idx
        return active_return

    def resample_portfolio_return(self, frequence):
        return self.portfolio_return.groupby(pd.TimeGrouper(freq=frequence)).agg(stats.cum_returns_final)

    def resample_benchmark_return(self, frequence):
        return self.benchmark_return.groupby(pd.TimeGrouper(freq=frequence)).agg(stats.cum_returns_final)

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
        df = self.portfolio_return.groupby(gfunc).agg(stats.cum_returns_final).rename(columns=['weekly_return'])
        df.index = idx
        return df

    @property
    def rel_weekly_performance(self):
        idx = GetDatetimeLastDayOfWeek(self.benchmark_return.index)
        gfunc = [lambda x:x.year, lambda x:x.isocalendar()[1]]
        df = self.benchmark_return.groupby(gfunc).agg(stats.cum_returns_final).rename(columns=['weekly_return'])
        df.index = idx
        return self.abs_weekly_performance - df

    @property
    def abs_monthly_performance(self):
        idx = GetDatetimeLastDayOfMonth(self.portfolio_return)
        gfunc = [lambda x: x.year, lambda x: x.month]
        df = self.portfolio_return.groupby(gfunc)['protfolio_return'].agg(
            [
                lambda x: stats.cum_returns_final(x) - stats.cum_returns_final(self.benchmark_return.reindex(x.index)),
                lambda x: np.std(x, ddof=1) * 20 ** 0.5,
                lambda x: stats.win_rate(x, self.benchmark_return, 'weekly'),
                lambda x: stats.win_rate(x, self.benchmark_return, 'daily')]
            ).rename(columns=['cum_return', 'volatility', 'weekly_win_rate', 'daily_win_rate'])
        df.index = idx
        return df

    @property
    def rel_monthly_performance(self):
        idx = GetDatetimeLastDayOfMonth(self.active_return)
        gfunc = [lambda x: x.year, lambda x: x.month]
        df = self.active_return.groupby(gfunc)['active_return'].agg(
            [
                lambda x: stats.cum_returns_final(x) - stats.cum_returns_final(self.benchmark_return.reindex(x.index)),
                lambda x: np.std(x, ddof=1) * 20 ** 0.5,
                lambda x: stats.win_rate(x, 0, 'weekly'),
                lambda x: stats.win_rate(x, 0, 'daily')]
        ).rename(columns=['cum_return', 'volatility', 'weekly_win_rate', 'daily_win_rate'])
        df.index = idx
        return df

    @property
    def abs_yearly_performance(self):
        idx = GetDatetimeLastDayOfYear(self.portfolio_return)
        gfunc = lambda x:x.year
        df = self.portfolio_return.groupby(gfunc).agg(
            [
                lambda x: stats.cum_returns_final(x) - stats.cum_returns_final(self.benchmark_return.reindex(x.index)),
                lambda x: np.std(x, ddof=1) * 250 ** 0.5,
                lambda x: stats.sharpe_ratio(x, simple_interest=True),
                stats.max_drawdown,
                lambda x: stats.information_ratio(x, self.benchmark_return),
                lambda x: stats.win_rate(x, self.benchmark_return, 'monthly'),
                lambda x: stats.win_rate(x, self.benchmark_return, 'weekly'),
                lambda x: stats.win_rate(x, self.benchmark_return, 'daily'),
            ]
        ).rename(columns=['cum_return', 'volatility', 'sharp_ratio', 'maxdd',
                          'IR', 'monthly_win_rate', 'weekly_win_rate', 'daily_win_rate'])
        df.index = idx
        return df

    @property
    def rel_yearly_performance(self):
        idx = GetDatetimeLastDayOfYear(self.active_return)
        gfunc = lambda x:x.year
        df = self.active_return.groupby(gfunc).agg(
            [
                lambda x: stats.cum_returns_final(x) - stats.cum_returns_final(self.benchmark_return.reindex(x.index)),
                lambda x: np.std(x, ddof=1) * 250 ** 0.5,
                lambda x: stats.sharpe_ratio(x, simple_interest=True),
                stats.max_drawdown,
                lambda x: stats.information_ratio(x, 0),
                lambda x: stats.win_rate(x, 0, 'monthly'),
                lambda x: stats.win_rate(x, 0, 'weekly'),
                lambda x: stats.win_rate(x, 0, 'daily'),
            ]
        ).rename(columns=['cum_return', 'volatility', 'sharp_ratio', 'maxdd',
                          'IR', 'monthly_win_rate', 'weekly_win_rate', 'daily_win_rate'])
        df.index = idx
        return df

    def range_pct(self, start, end, rel=True):
        if rel:
            return stats.cum_returns_final(self.active_return.loc[start:end])
        else:
            return stats.cum_returns_final(self.portfolio_return.loc[start:end])

    def portfolio_weights(self, date):
        return



