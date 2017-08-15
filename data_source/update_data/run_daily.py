
from data_source import data_source
from data_source.update_data.value_factor import ValueFuncListDaily
from data_source.update_data.momentum_factor import MomentumFuncListDaily
from data_source.update_data.liquidity_factor import LiquidityFuncListDaily
from data_source.update_data.reverse_fator import ReverseFuncListDaily
from data_source.update_data.time_series_factor import TimeSeriesFuncListDaily
from data_source.update_data.alternative_factor import AlternativeFuncListDaily


def dailyfactors(start, end):
    #更新价值类因子数据
    for func in ValueFuncListDaily:
        func(start, end, data_source=data_source)

    #更新动量类因子
    for func in MomentumFuncListDaily:
        func(start, end, data_source=data_source)

    #更新流动性数据
    for func in LiquidityFuncListDaily:
        func(start, end, data_source=data_source)

    #更新反转因子
    for func in ReverseFuncListDaily:
        func(start, end, data_source=data_source)

    #更新时间序列类因子
    for func in TimeSeriesFuncListDaily:
        func(start, end, data_source=data_source)

    #更新另类因子
    for func in AlternativeFuncListDaily:
        func(start, end, data_source=data_source)