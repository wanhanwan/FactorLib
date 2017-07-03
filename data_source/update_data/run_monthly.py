from data_source import data_source

start = '20170601'
end = '20170701'

#更新价值类因子数据
from value_factor import ValueFuncListMonthly
for func in ValueFuncListMonthly:
    func(start, end, data_source=data_source)

#更新动量类因子
from momentum_factor import MomentumFuncListMonthly
for func in MomentumFuncListMonthly:
    func(start, end, data_source=data_source)

#更新流动性数据
from liquidity_factor import LiquidityFuncListMonthly
for func in LiquidityFuncListMonthly:
    func(start, end, data_source=data_source)

#更新反转因子
from reverse_fator import ReverseFuncListMonthly
for func in ReverseFuncListMonthly:
    func(start, end, data_source=data_source)

#更新时间序列类因子
from time_series_factor import TimeSeriesFuncListMonthly
for func in TimeSeriesFuncListMonthly:
    func(start, end, data_source=data_source)

#更新另类因子
from alternative_factor import AlternativeFuncListMonthly
for func in AlternativeFuncListMonthly:
    func(start, end, data_source=data_source)