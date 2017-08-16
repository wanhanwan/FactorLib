import os
import pandas as pd
from utils.strategy_manager import StrategyManager, update_nav, collect_nav
from utils.excel_io import write_xlsx


sm = StrategyManager('D:/data/factor_investment_strategies', 'D:/data/factor_investment_stocklists')
pf=sm.performance_analyser(strategy_name='兴基VG_逆向_行业中性')
dff=pd.concat([pf.portfolio_return, pf.benchmark_return, pf.active_return], axis=1)
write_xlsx(r"D:\data\EXCEL\兴基VG_逆向_行业中性.xlsx", returns=dff, yearly_returns=pf.rel_yearly_performance)
# sm.backup()
# update_nav('20170809', '20170811')
# for f in [x for x in os.listdir('.') if x != 'summary.csv']:
    # sm.create_from_directory(os.path.abspath(f))
    # sm.update_stocks('20070101', '20170731', strategy_name=f)
    # sm.run_backtest('20170809', '20170811', strategy_name=f)
# sm.run_backtest('20070131', '20170808', strategy_name='兴业风格_情绪')