from data_source.update_data.update_h5db_base_data import (onlist,
                                                           update_price,
                                                           update_sector,
                                                           update_trade_status,
                                                           update_idx_weight,
                                                           update_industry_name)
from data_source.update_data.run_daily import dailyfactors
from data_source.wind_financial_data_api import update
from data_source import h5
from datetime import datetime, time
import pandas as pd
import os

latest_update_date = '20170811'

start = '20170809'
end = '20170809'
UpdateFuncs = [onlist,
               update_price,
               update_sector,
               update_trade_status,
               update_idx_weight,
               update_industry_name
               ]

while 1:
    if datetime.today().date() > datetime.strptime(latest_update_date, '%Y%m%d'):
        flag0 = 1
        flag1 = 1
    if datetime.now().time() > time(18, 0, 0) and flag0:
        for iFunc in UpdateFuncs:
            iFunc(start, end)
        update.update_all(start, end)
        dailyfactors(start, end)
        h5.snapshot(pd.date_range(start, end), 'base_factor', mail=True)
        flag0 = 0
    if datetime.now().time() > time(20, 0, 0) and flag1:
        os.system("rqalpha update_bundle")
        flag1 = 0
    latest_update_date = datetime.today().date().strftime("%Y%m%d")
