import pandas as pd
from report_ann_date import get_latest_report_date, get_nlatest_report_dates
from data_source import h5


def _reindex(data, idx):
    idx2 = idx.reset_index(level=1)
    idx2.index = pd.DatetimeIndex(idx2.index)
    new_data = idx2.join(data, on=['max_report_date', 'IDs'])
    return new_data.set_index('IDs', append=True)

def _load_data(field, idx):
    """加载财务数据，并重索引"""
    raw_data = h5.load_factor(field, '/stock_financial_data/')
    return _reindex(raw_data, idx)