from sqlalchemy import create_engine

from .wind_db import WindDB
from .local_db import LocalDB
from .mongodb import mongoDB
from .trade_calendar import trade_calendar
from .base_data_source import base_data_source, sector


mongo = mongoDB()
tc = trade_calendar()
sec = sector(mongo, tc)
base_data_source = base_data_source(sec)

# mysql引擎
mysql_engine = create_engine('mysql+pymysql://root:123456@localhost/barrafactors')
