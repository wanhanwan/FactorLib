from missing_value_manage import missing_value_func_dict
from grouping_manage import grouping_funcs_dict
from utils.factor_result_saving import *
import copy
import os
import shutil

class factor_operator(object):
    def __init__(self, env):
        self._env = env
        self._config = env._config

    def load_factor(self,factor):
        """从数据库中读取数据"""
        sector_name = self._config.universe
        start_date = self._config.start_date
        end_date = self._config.end_date
        dates = self._env._trade_calendar.get_trade_days(start_date, end_date, self._config.freq)
        if sector_name == '全A':
            stocks = self._env._sector.get_history_ashare(dates)
        else:
            stocks = self._env._sector.get_index_members(sector_name,dates=dates)
        ids = stocks.index.levels[1].unique().tolist()
        factor_data = self._env._data_source.load_factor(factor.axe,ids=ids,dates=dates)
        factor.data = factor_data.reindex(stocks.index)
        factor.update_info()

    def drop_nan(self, factor, inplace=False):
        """处理缺失值"""
        try:
            nan_func = missing_value_func_dict[self._config.nan_method]
        except Exception:
            raise KeyError("method not supported")
        new_data = nan_func(factor)
        if inplace:
            factor.data = new_data
            factor.update_info()
            return
        else:
            new_factor = self.copy(factor,factor.name+'_nan_dropped')
            new_factor.data = new_data
            new_factor.update_info()
        return new_factor

    def drop_untradable_stocks(self,factor, inplace=False):
        """去掉不可以交易的股票"""
        all_dates = factor.data.index.levels[0].tolist()
        stocks_trade_status = self._env._data_source.get_stock_trade_status(dates=all_dates)
        common = factor.data.merge(stocks_trade_status[['no_trading']],
                                   left_index=True,right_index=True,how='left')
        new_data = common[common['no_trading']!=True][[factor.axe]]
        if inplace:
            factor.data = new_data
            factor.update_info()
            return
        else:
            new_factor = self.copy(factor,factor.name+'_nan_dropped')
            new_factor.data = new_data
            new_factor.update_info()
        return new_factor

    def grouping(self,factor):
        methods = self._config.grouping_method
        for method in methods:
            grouping_funcs_dict[method](factor,self._env._data_source, self._config)
    
    def gen_stock_list(self, factor):
        return

    def copy(self,factor,new_name=None):
        """复制一个因子"""
        copy_of_factor = copy.deepcopy(factor)
        copy_of_factor.name = new_name
        return copy_of_factor

    # ---------因子存储----------
    def save_factor(self, factor):
        base_dir = os.path.abspath(self._config.result_file_dir)
        file_dir = base_dir + os.sep + factor.name
        if os.path.isdir(file_dir):
            shutil.rmtree(file_dir)
            os.makedirs(file_dir)
        else:
            os.makedirs(file_dir)
        # 存储因子IC序列的图片
        save_ic_figure(factor, file_dir,self._config,self._env)
        save_group_return_figure(factor, file_dir, self._config, self._env)
        save_details_to_excel(factor, file_dir, self._config, self._env)
        save_factor_as_pickle(factor, file_dir, self._config, self._env)
        save_group_info_as_mat(factor, file_dir, self._config, self._env)
        
        








