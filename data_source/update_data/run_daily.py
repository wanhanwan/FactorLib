
from data_source import data_source
from value_factor import ValueFuncListDaily
from momentum_factor import MomentumFuncListDaily
from liquidity_factor import LiquidityFuncListDaily
from reverse_fator import ReverseFuncListDaily
from time_series_factor import TimeSeriesFuncListDaily
from alternative_factor import AlternativeFuncListDaily


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