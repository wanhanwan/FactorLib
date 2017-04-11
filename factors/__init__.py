from contextlib import closing
from factors.panel_factor import panelFactor
from environment import Environment
from single_factor_test.config import parse_config
from utils import AttrDict
import shelve
import os


def load_factor(factor_name, file_path):
    """加载因子"""
    file_name = file_path + os.sep + factor_name + os.sep + factor_name + '_data'
    if not os.path.isfile(file_name+'.dat'):
        raise KeyError("no such a factor {factor}".format(factor=factor_name))
    with closing(shelve.open(file_name)) as f:
        _env = f['env']
        panelFactor.set_measurer(_env)
        panelFactor.set_operator(_env)
        
        factor = panelFactor(*f['factor_tuple'])
        factor.group_return = f['group_return']
        factor.grouping_info = f['group_info']
        factor.ic_series = f['ic_series']
        factor.data = f['factor_data']
        factor.first_group_active_return = f['first_group_active_return']
    return factor