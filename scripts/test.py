import os
from utils.strategy_manager import StrategyManager


sm = StrategyManager('D:/data/factor_investment_strategies', 'D:/data/factor_investment_stocklists')
os.chdir(r"D:\data\factor_investment_strategies")
# sm.backup()
for f in [x for x in os.listdir('.') if x != 'summary.csv']:
    # sm.create_from_directory(os.path.abspath(f))
    # sm.update_stocks('20070101', '20170731', strategy_name=f)
    sm.run_backtest('20170809', '20170811', strategy_name=f)
# sm.run_backtest('20070131', '20170808', strategy_name='兴业风格_情绪')