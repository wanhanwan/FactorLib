from data_source.base_data_source import base_data_source,sector
from data_source.trade_calendar import trade_calendar
from data_source.mongodb import mongoDB
tc = trade_calendar()
mog = mongoDB()
sec = sector(mog,tc)
data_source = base_data_source(mog,sec)

# data = data_source.get_period_return(['000001','000002'],'20100104','20100108')
# data = data_source.get_fix_period_return(['000001','000002'],freq='1m',start_date='20100104',end_date='20110108')
# h5.merge_db('stlist')
# data_source.get_stock_trade_status(dates=['20100104','20100105'])

