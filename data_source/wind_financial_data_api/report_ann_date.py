from WindPy import *
from utils.tool_funcs import ReportDateAvailable, windcode_to_tradecode
from data_source import h5
from data_source.update_data.update_h5db_base_data import get_ashare
import pandas as pd


w.start()

def update_report_ann_dt(start, end):
    """更新报告期和公告期"""
    stocks = get_ashare(end)
    report_dates = ReportDateAvailable(start, end)
    ann_dates = []
    for date in report_dates:
        d = w.wsd(stocks, "stm_issuingdate", date, date, "Period=Q;Days=Alldays")
        iann_dates = d.Data[0]
        iann_dates = [x.strftime("%Y%m%d") if x is not None else x for x in iann_dates]
        ann_dates.append(iann_dates)
    tradecodes = [windcode_to_tradecode(x) for x in stocks]
    dates = pd.DatetimeIndex(report_dates, name='date')
    report_ann_dates = pd.DataFrame(ann_dates, index=dates, columns=tradecodes)
    report_ann_dates = report_ann_dates.stack().to_frame().rename(columns={0:'ann_dt'})
    report_ann_dates.index.names = ['date', 'IDs']
    h5.save_factor(report_ann_dates, '/stocks/')
    return

def get_latest_report_date(dates, report_type=None):
    """特定日期序列的最大报告期"""
    ann_report_dates = _get_report_ann_dt(report_type)
    _ = []
    if isinstance(dates, str):
        dates = [dates]
    for date in dates:
        temp = ann_report_dates[ann_report_dates['ann_dt']<=date]
        max_date = temp.groupby('IDs')['date'].max()
        max_date.index = pd.MultiIndex.from_product([[date], max_date.index], names=['date', 'IDs'])
        _.append(max_date)
    return pd.concat(_).rename('max_report_date').to_frame()

def get_nlatest_report_dates(dates, n, report_type=None):
    """特定日期序列的前n个最大报告期"""
    ann_report_dates = _get_report_ann_dt(report_type)
    _ = []
    if isinstance(dates, str):
        dates = [dates]
    for date in dates:
        temp = ann_report_dates[ann_report_dates['ann_dt']<=date]
        max_date = temp.groupby('IDs')['date'].nlargest(n).reset_index(level=1, drop=True)
        max_date.index = pd.MultiIndex.from_product([[date], max_date.index], names=['date', 'IDs'])
        _.append(max_date)
    return pd.concat(_).rename('max_report_date').to_frame()   

def _get_report_ann_dt(report_type):
    ann_report_dates = h5.load_factor('ann_dt', '/stock_financial_data/').reset_index(level=0)
    if report_type is None:
        return ann_report_dates
    elif report_type == 'Q1':
        return ann_report_dates[pd.DatetimeIndex(ann_report_dates['date']).month == 3]
    elif report_type == 'Q2':
        return ann_report_dates[pd.DatetimeIndex(ann_report_dates['date']).month == 6]
    elif report_type == 'Q3':
        return ann_report_dates[pd.DatetimeIndex(ann_report_dates['date']).month == 9]
    else:
        return ann_report_dates[pd.DatetimeIndex(ann_report_dates['date']).month == 12]        
        
if __name__ == '__main__':
    d = update_report_ann_dt('20070101', '20170629')
