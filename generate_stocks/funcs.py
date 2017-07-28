from data_source import data_source
from riskmodel import stockpool
from QuantLib.utils import DropOutlier, Standard
import numpy as np
import pandas as pd


def _load_factors(factor_dict, stocklist):
    dates = stocklist.index.get_level_values(0).unique()
    data = data_source.h5DB.load_factors(factor_dict, dates=list(dates))
    return data.reindex(stocklist).dropna()


def _rawstockpool(sectorid, dates):
    stockpool = data_source.sector.get_index_members(sectorid, dates=dates)
    return stockpool


def _stockpool(sectorid, dates, qualify_method):
    raw = _rawstockpool(sectorid, dates)
    return stockpool._qualify_stocks(raw, qualify_method)


def _drop_outlier(factors, method):
    newfactors = factors.apply(lambda x: DropOutlier(
        x.reset_index(), x.name, method=method, alpha=2)[[x.name+'_after_drop_outlier']])
    newfactors = newfactors.rename(columns=lambda x: x.replace("_after_drop_outlier", ""))
    return newfactors


def _standard(factors):
    newfactors = factors.apply(lambda x: Standard(x.reset_index(), x.name)[[x.name+'_after_standard']])
    newfactors = newfactors.rename(columns=lambda x: x.replace('_after_standard', ''))
    return newfactors


def _total_score(factors, weight=None):
    n_factors = factors.shape[1]
    if weight is None:
        weight = np.array([1/n_factors]*n_factors)[:, np.newaxis]
    else:
        weight = np.array(weight)[:, np.newaxis]
    total_score = np.dot(factors.data, weight)
    return pd.DataFrame(total_score, index=factors.index, columns=['total_score'])


