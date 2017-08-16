from data_source import data_source
from riskmodel import stockpool
from QuantLib.utils import DropOutlier, Standard, ScoringFactors
from utils.tool_funcs import parse_industry
import numpy as np
import pandas as pd


def _load_factors(factor_dict, stocklist):
    dates = stocklist.index.get_level_values(0).unique()
    data = data_source.h5DB.load_factors(factor_dict, dates=list(dates))
    return data.reindex(stocklist.index).dropna()


def _rawstockpool(sectorid, dates):
    stockpool = data_source.sector.get_index_members(sectorid, dates=dates)
    return stockpool


def _stockpool(sectorid, dates, qualify_method):
    raw = _rawstockpool(sectorid, dates)
    return stockpool._qualify_stocks(raw, qualify_method)


def _drop_outlier(factors, method):
    newfactors = factors.apply(lambda x: DropOutlier(
        x.reset_index(), x.name, method=method, alpha=2)[x.name+'_after_drop_outlier'])
    newfactors = newfactors.rename(columns=lambda x: x.replace("_after_drop_outlier", ""))
    return newfactors


def _standard(factors):
    newfactors = factors.apply(lambda x: Standard(x.reset_index(), x.name)[x.name+'_after_standard'])
    newfactors = newfactors.rename(columns=lambda x: x.replace('_after_standard', ''))
    return newfactors


def score_by_industry(factor_data, industry_name, factor_names=None, **kwargs):
    factor_names = factor_names if factor_names is not None else list(factor_data.columns)
    industry_str = parse_industry(industry_name)
    all_ids = factor_data.index.get_level_values(1).unique().tolist()
    all_dates = factor_data.index.get_level_values(0).unique().tolist()

    # 个股的行业信息与因子数据匹配
    industry_info = data_source.sector.get_stock_industry_info(
        all_ids, industry=industry_name, dates=all_dates).reset_index()
    factor_data = factor_data.reset_index()
    factor_data = pd.merge(factor_data, industry_info, how='left')
    score = factor_data.set_index(['date', 'IDs']).groupby(
        industry_str, group_keys=False).apply(ScoringFactors, factors=factor_names, **kwargs)
    return score


def score_typical(factor_data, factor_names=None, **kwargs):
    factor_names = factor_names if factor_names is not None else list(factor_data.columns)
    score = ScoringFactors(factor_data, factor_names)
    return score


def _total_score(factors, directions, weight=None):
    n_factors = factors.shape[1]
    if weight is None:
        weight = np.array([1/n_factors]*n_factors)[:, np.newaxis]
    else:
        weight = np.array(weight)[:, np.newaxis]
    directions = np.array([directions[x] for x in directions])
    total_score = np.dot(factors.values*directions, weight)
    return pd.DataFrame(total_score, index=factors.index, columns=['total_score'])


def _to_factordict(factors):
    _dict = {}
    direction = {}
    for factor in factors:
        if factor[1] not in _dict:
            _dict[factor[1]] = [factor[0]]
        else:
            _dict[factor[1]].append(factor[0])
        direction[factor[0]] = factor[2]
    return _dict, direction


if __name__ == "__main__":
    dates = data_source.trade_calendar.get_trade_days('20170701', '20170801', '1m')
    factors = data_source.h5DB.load_factors({'/stocks/':['float_mkt_value']}, dates=dates)
    score = score_by_industry(factors, '中信一级', method='Mean-Variance')




