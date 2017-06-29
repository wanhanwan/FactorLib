from WindPy import *
from data_source import h5
from utils.tool_funcs import ReportDateAvailable, windcode_to_tradecode
from data_source.update_data.update_h5db_base_data import get_ashare
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
    h5.save_factor(data, '/stock_financial_data/')

if __name__ == '__main__':
    update_data('roe_ttm2', '20070101', '20170629')