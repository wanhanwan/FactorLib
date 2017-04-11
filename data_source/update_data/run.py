from update_wind_data import updateFuncs
from datetime import datetime
from datetime import timedelta

for iFunc in updateFuncs:
    iFunc('20170406', '20170410')

        
