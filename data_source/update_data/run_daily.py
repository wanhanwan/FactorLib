
from data_source import data_source

start = '20170701'
end = '20170709'

#更新价值类因子数据
from value_factor import ValueFuncListDaily
for func in ValueFuncListDaily:
    func(start, end, data_source=data_source)

#更新动量类因子
from momentum_factor import MomentumFuncListDaily
for func in MomentumFuncListDaily:
    func(start, end, data_source=data_source)

#更新流动性数据
from liquidity_factor import LiquidityFuncListDaily
for func in LiquidityFuncListDaily:
    func(start, end, data_source=data_source)
    
#更新反转因子
from reverse_fator import ReverseFuncListDaily
for func in ReverseFuncListDaily:
    func(start, end, data_source=data_source)

#更新时间序列类因子
from time_series_factor import TimeSeriesFuncListDaily
for func in TimeSeriesFuncListDaily:
    func(start, end, data_source=data_source)

#更新另类因子
from alternative_factor import AlternativeFuncListDaily
for func in AlternativeFuncListDaily:
    func(start, end, data_source=data_source)