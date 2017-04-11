import pandas as pd
import numpy as np
from factor_performance.toolfuncs import *
from empyrical import stats
import pyfolio as pf
import pickle

'''给定收益率序列，计算相关指标'''


class FundPerformance(object):

    """docstring for FundPerformance"""

    def __init__(self, yield_series, benchmark_ret={}, benchmark_price={}, turn_over=[],
                 position=[], benchmark_used=DEFAULT_BENCHMARK, freq='daily', free_rate=0.,
                 date=[]):

        super(FundPerformance, self).__init__()
        self.Ret = set_datetime_index(yield_series)
        self.RetUsed = self.Ret
        self.Freq = freq
        self.FreeRate = free_rate

        benchmark_ret = {i: set_datetime_index(
            benchmark_ret[i]) for i in benchmark_ret}

        benchmark_price = {i: set_datetime_index(
            benchmark_price[i]) for i in benchmark_price}

        self.BenchmarkRets = benchmark_ret
        self.BenchmarkUsed = benchmark_used
        self.BenchmarkPrice = benchmark_price

        self.BenchmarkRetUsed = get_benchmark_return(
            self.BenchmarkRets, self.BenchmarkUsed)

        self.activeRet = stats._adjust_returns(
            self.RetUsed, self.BenchmarkRetUsed).reindex(self.RetUsed.index)

        self.Position = position
        self.TurnOver = turn_over
        self.Date = date

    # 设置日期
    def SetTimeRange(self, start, end):
        self.RetUsed = self.Ret.ix[start:end]
        self.activeRet = stats._adjust_returns(
            self.RetUsed, self.BenchmarkRetUsed).reindex(self.RetUsed.index)
        return 1

    # 设置收益率序列
    def SetYieldSeries(self, yield_series, freq='daily'):
        self.RetUsed = set_datetime_index(yield_series)
        self.activeRet = stats._adjust_returns(
            self.RetUsed, self.BenchmarkRetUsed).reindex(self.RetUsed.index)
        self.Freq = freq
        return 1

    # 设置对冲标的
    def SetBenchmark(self, benchmark_name):
        self.BenchmarkUsed = benchmark_name
        self.BenchmarkRetUsed = get_benchmark_return(
            self.BenchmarkRets, self.BenchmarkUsed)
        self.activeRet = stats._adjust_returns(
            self.RetUsed, self.BenchmarkRetUsed).reindex(self.RetUsed.index)
        return 1

    # 设置无风险收益率
    def SetFreeRate(self, free_rate):
        self.FreeRate = free_rate
        return 1

    # 计算年化收益
    def AnualYield(self, hedge=True):
        ret = self.activeRet if hedge else self.RetUsed
        return stats.annual_return(ret,
                                   period=self.Freq)

    # 计算最大回撤
    def MaxDrawDown(self, hedge=True):
        ret = self.activeRet if hedge else self.RetUsed
        return stats.max_drawdown(ret)

    # 计算年化波动率
    def AnualVol(self, hedge=True):
        ret = self.activeRet if hedge else self.RetUsed
        return stats.annual_volatility(ret,
                                       period=self.Freq)

    # 计算夏普比率
    def SharpRatio(self, simple_interest=True, hedge=True):
        ret = self.activeRet if hedge else self.RetUsed
        return stats.sharpe_ratio(ret,
                                  risk_free=self.FreeRate,
                                  period=self.Freq,
                                  simple_interest=simple_interest)

    # 计算月收益率
    def MonthlyRet(self, hedge=True):
        month_year = pd.Series(self.RetUsed.index).apply(
            lambda x: x.year * 100 + x.month)
        is_month_end = (~month_year.duplicated('last')).tolist()
        if hedge:
            monthRet = pd.Series(
                stats.aggregate_returns(self.activeRet, 'monthly'))
            monthRet.index = self.activeRet.index[is_month_end]
        else:
            monthRet = pd.Series(stats.aggregate_returns(self.RetUsed, 'monthly'))
            monthRet.index = self.RetUsed.index[is_month_end]
        return monthRet

    # 计算基准的月收益率
    def BenchmarkMonthlyRet(self):
        benchmarkRet = self.BenchmarkRetUsed.reindex(self.RetUsed.index)
        month_year = pd.Series(self.RetUsed.index).apply(
            lambda x: x.year * 100 + x.month)
        is_month_end = (~month_year.duplicated('last')).tolist()
        monthRet = pd.Series(stats.aggregate_returns(benchmarkRet, 'monthly'))
        monthRet.index = self.RetUsed.index[is_month_end]
        return monthRet

    # 计算胜率
    def WinRate(self, period='monthly'):

        return stats.win_rate(self.RetUsed, self.BenchmarkRetUsed, period=period)

    # 对收益率进行统计汇总
    def PerfStats(self):

        # benchmarkRet = get_benchmark_return(self.BenchmarkRets, benchmark_name)
        return pyfolio.timeseries.perf_stats(self.RetUsed, self.BenchmarkRetUsed)

    # 画出净值图
    def PlotNav(self, benchmark_name=DEFAULT_BENCHMARK):

        activrCumRet = pd.Series(stats.cum_returns(self.activeRet),
                                 name="Excess Return") + 1

        ax = pf.plot_rolling_returns(
            self.RetUsed, factor_returns=self.BenchmarkRetUsed)
        ax2 = ax.twinx()
        ax2 = activrCumRet.plot(ax=ax2,
                                label="Excess Return",
                                color='b',
                                alpha=0.6)
        ax2.set_ylabel("Excess Cum Return")
        lin1, labels1 = ax.get_legend_handles_labels()
        lin2, labels2 = ax2.get_legend_handles_labels()

        ax.legend(lin1+lin2, labels1+labels2, loc=0)
    
    def YearlyPerformance(self):
        monthlyRet = self.MonthlyRet(False)                         # 单边做多模型月度收益率
        benchmarkMonthlyRet = self.BenchmarkMonthlyRet()
        hedgeMonthlyRet = self.MonthlyRet(True)                     # 对冲组合月收益率
        
        a = stats.aggregate_returns(monthlyRet, 'yearly')           # 多头组合分年收益
        b = stats.aggregate_returns(benchmarkMonthlyRet, 'yearly')  # 基准组合分年收益
        
        _l = []
        for i, year in enumerate(a.index):
            hedgeMonthlyRet_current_year = hedgeMonthlyRet.ix[str(year)]
            monthlyRet_current_year = monthlyRet.ix[str(year)]
            benchmarkMonthlyRet_current_year = benchmarkMonthlyRet.ix[str(year)]
            
            hdSharp_current_year = stats.sharpe_ratio(
                hedgeMonthlyRet_current_year, annualization=12)
            hdMaxDown_current_year = stats.max_drawdown(hedgeMonthlyRet_current_year)
            hdReturn_current_year = stats.annual_return(hedgeMonthlyRet_current_year, annualization=12)
            hdWinRate_current_year = stats.win_rate(monthlyRet_current_year, benchmarkMonthlyRet_current_year)
            _l.append([hdSharp_current_year, hdReturn_current_year, hdMaxDown_current_year, hdWinRate_current_year])
        # 计算全年收益表现
        hdSharp_all = stats.sharpe_ratio(hedgeMonthlyRet, annualization=12)
        hdMaxDown_all = stats.max_drawdown(hedgeMonthlyRet)
        hdReturn_all = stats.annual_return(hedgeMonthlyRet, annualization=12)
        hdWinRate_all = stats.win_rate(hedgeMonthlyRet, benchmarkMonthlyRet)
        _l.append([hdSharp_all, hdReturn_all, hdMaxDown_all, hdWinRate_all])
        result = pd.DataFrame(_l, columns=['夏普比率', '年化收益', '最大回撤', '胜率'], index=list(a.index) + ['All'])
        return result

