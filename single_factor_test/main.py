# 单因子测试主函数

from single_factor_test.config import parse_config

from utils import AttrDict
from environment import Environment
from factors.panel_factor import panelFactor

import importlib.util as ilu


def run(config_path):
    # 加载设置文件
    config = parse_config(config_path)
    config = AttrDict(config)

    # 初始化运行环境
    print("初始化运行环境...")
    _env = Environment(config)
    _env._initialize()

    # 为因子设置环境变量
    panelFactor.set_measurer(_env)
    panelFactor.set_operator(_env)

    # 加载因子
    factors = load_factors(config)
    for factor in factors:
        f = panelFactor(*factor)
        print("正在测试因子【{name}】".format(name=f.name))
        f.start_back_test()

def load_factors(config):
    spec = ilu.spec_from_file_location("factor_list", config.factor_list_file)
    factor_list = ilu.module_from_spec(spec)
    spec.loader.exec_module(factor_list)
    return factor_list.factor_list

run("D:/FactorLib/single_factor_test/config.yml")