import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import ticker

from empyrical import stats
import numpy as np
import pandas as pd

def plot_ic_returns(factor,config,env):
    """绘制因子收益图
    分为三行: 1.因子IC柱状图 2. 多空月度收益率柱状图 3.多空净值曲线
    """
    fig, axes = plt.subplots(3,1,figsize=(8,6))
    plt.subplots_adjust(0.1, 0.1, 0.9, 0.9, hspace=0.3)
    ic = factor.ic_series


    # IC柱状图
    my_color = ['r' if x > 0 else 'g' for x in ic]
    ic.plot(kind='bar', ax=axes[0], color=my_color)
    _adjust_date_axis(axes[0], ic.index)
    axes[0].set_title("IC Series", fontsize=9)
        
    # 多空月度收益率柱状图
    _l = []
    for _m in factor.long_short_return:    
        pf = factor.long_short_return[_m]
        _l.append(pf.activeRet)
    long_short_returns = pd.concat(_l, axis=1)
    long_short_returns.columns = list(factor.long_short_return.keys())
    long_short_m_returns = long_short_returns.apply(stats.aggregate_returns,args=('monthly',))
    new_index = [x * 100 + y for x, y in long_short_m_returns.index]

    long_short_m_returns.plot(kind='bar',ax=axes[1])
    _adjust_date_axis(axes[1], new_index)
    axes[1].set_title("Monthly Return of Long-Short Protfolio", fontsize=9)

    # 多空净值曲线
    long_short_cum_returns = long_short_returns.apply(stats.cum_returns,args=(1,))
    long_short_cum_returns.plot(linewidth=2, ax=axes[2])
    axes[2].set_title("Net Value of Long-Short Portfolio", fontsize=9)

    return fig, axes

def plot_group_return(factor,config,env):
    """绘制因子分组收益柱状图"""
    
    # 这里需要使用超额收益率，所以得加载基准收益率
    #benchmark_anual_return = stats.annual_return(env.benchmark_return['returns'])
    
    group_anual_returns = []
    for _m in factor.group_return:
        _g = factor.group_return[_m].copy()
        _g.index = pd.DatetimeIndex(_g.index)
        benchmark_anual_return = stats.annual_return(_g.mean(axis=1))
        r = _g.apply(stats.annual_return) - benchmark_anual_return
        r.name = _m
        group_anual_returns.append(r)
    group_anual_returns = pd.concat(group_anual_returns,axis=1)
    group_anual_returns.columns = list(factor.group_return.keys())
    group_anual_returns.index = group_anual_returns.index.astype('int32')
    ax = group_anual_returns.plot(
        kind='bar', title="Excess Return".format(benchmark=config.benchmark))
    return ax.figure, ax

def _adjust_date_axis(ax, date_seq):
    tick_labels = [""] * len(date_seq)
    show = 6
    step = int(len(date_seq) / show)
    try:
        tick_labels[::step] = [x.to_pydatetime().strftime("%Y%m") for x in date_seq[::step]]
    except:
        tick_labels[::step] = list(map(str, date_seq[::step]))
    ax.xaxis.set_major_formatter(ticker.FixedFormatter(tick_labels))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, horizontalalignment='right', fontsize=7)
    ax.set_xlabel("")
    #ax.figure.autofmt_xdate()