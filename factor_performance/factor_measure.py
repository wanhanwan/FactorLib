import pandas as pd
from .plotting import *
from .fund_performance import FundPerformance

class factor_measurer(object):
    def __init__(self, env):
        self._env= env
        self._config = env._config

    def get_ic(self,factor):
        """计算因子IC
        计算起初因子与本次持仓周期收益率的秩相关系数
        """
        data = factor.data
        all_dates = factor.data.index.levels[0]

        _ic = []
        for i, date in enumerate(all_dates[1:]):
            section_factor = data.ix[all_dates[i]]
            cum_returns = self._env._data_source.get_period_return(section_factor.index.tolist(),
                                                                   all_dates[i],date)
            common = pd.concat([section_factor,cum_returns],axis=1)
            _ic.append(common.corr(method='spearman').iloc[0,1])
        factor.ic_series = pd.Series(_ic,index=all_dates[1:])

    def get_group_returns(self,factor):
        """从因子的grouping_info中计算分组的收益率序列
        """
        if not factor.grouping_info:
            raise ValueError("factor.grouping_info is empty!")

        start_date = self._config.start_date
        end_date = self._config.end_date
        all_dates = self._env._data_source.trade_calendar.get_trade_days(start_date,end_date)
        stock_returns = self._env._data_source.load_factor('daily_returns',start_date=start_date,end_date=end_date)

        group_return = {}
        for method in factor.grouping_info:
            group_info = factor.grouping_info[method].unstack().sort_index()
            _g = group_info.reindex(pd.DatetimeIndex(all_dates,name='date'),method='ffill').stack()
            common = pd.merge(_g,stock_returns,how='left',left_index=True,right_index=True)
            common.reset_index(inplace=True)

            _r = common.groupby(['date','group_id'])['daily_returns'].mean()
            group_return[method] = _r.unstack().sort_index()
        factor.group_return = group_return

    def get_long_short_return(self,factor):
        """获得每一种分类方法的多空收益率"""
        long_short_returns = {}
        for _m in factor.group_return:
            group_return = factor.group_return[_m].fillna(method='ffill', axis=1)
            r_long = group_return.iloc[:, 0]
            r_short = group_return.iloc[:, -1]
            pf = FundPerformance(r_long, benchmark_ret={'group_10': r_short},
                                 benchmark_used='group_10')
            long_short_returns[_m] = pf
        factor.long_short_return = long_short_returns
    
    def get_first_group_active_return(self, factor):
        first_group_active_return = {}
        
        # 这里需要使用超额收益率，所以得加载基准收益率
        benchmark_return = self._env.benchmark_return
        
        for _m in factor.group_return:
            r = factor.group_return[_m].iloc[:, 0]
            pf = FundPerformance(r, benchmark_ret={self._config.benchmark: benchmark_return['returns']},
                                 benchmark_used=self._config.benchmark)
            first_group_active_return[_m] = pf
        factor.first_group_active_return = first_group_active_return
    
    def plot_performance(self,factor):
        fig,ax = plot_group_return(factor,self._config,self._env)
        factor.group_return_fig = fig
        fig,ax = plot_ic_returns(factor,self._config,self._env)
        factor.ic_fig = fig



