from WindPy import *
from data_source import save_factor
from utils.tool_funcs import ReportDateAvailable, windcode_to_tradecode
from FactorLib.data_source.update_data.update_h5db_base_data import get_ashare
import pandas as pd

w.start()


def update_data(field, start, end):
    stocks = get_ashare(end)
    report_dates = ReportDateAvailable(start, end)
    _ = []
    for date in report_dates:
        d = w.wsd(stocks, field, date, date, "Period=Q;Days=Alldays")
        data = d.Data[0]
        _.append(data)
    tradecodes = [windcode_to_tradecode(x) for x in stocks]
    dates = pd.DatetimeIndex(report_dates, name='date')
    data = pd.DataFrame(_, index=dates, columns=tradecodes).stack().to_frame().rename(columns={0:field})
    data.index.names = ['date', 'IDs']
    save_factor(data, '/stock_financial_data/')

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
    save_factor(report_ann_dates, '/stock_financial_data/')
    return


if __name__ == '__main__':
    # update_report_ann_dt('20050101','20070101')
    update_data('or_ttm2', '20061001', '20070101')
    update_data('roe_ttm2', '20050101', '20070101')