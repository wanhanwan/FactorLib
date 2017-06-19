import pandas as pd
import os

from utils.datetime_func import getWeekLastDay,getWeekFirstDay,getMonthFirstDay,getMonthLastDay, Datetime2DateStr

class trade_calendar(object):
    allTradeDays = pd.read_csv(
        os.sep.join(__file__.split(os.sep)[:-2])+os.sep+'resource/trade_dates.csv',
        index_col=0, header=None, squeeze=True, dtype='str').values.tolist()

    def get_trade_days(self, start_date=None,end_date=None,freq='1d',first_or_last='L'):
        """获得交易日期
        对于freq的计算方式:
        '1d':前面数字表示间隔数目,后面字母表示频率,频率包括(d,w,m,y)
        """
        step = int(freq[0])
        if freq[1] == 'd':
            Days = trade_calendar.allTradeDays
        elif freq[1] == 'w':
            if first_or_last == 'L':
                Days = getWeekLastDay(trade_calendar.allTradeDays, step=step)
            elif first_or_last == 'F':
                Days = getWeekLastDay(trade_calendar.allTradeDays, step=step)
        elif freq[1] == 'm':
            if first_or_last == 'L':
                Days = getMonthLastDay(trade_calendar.allTradeDays, step=step)
            elif first_or_last == 'F':
                Days = getMonthFirstDay(trade_calendar.allTradeDays, step=step)
        else:
            raise KeyError('type of freq only supports d, w and m')
        Rslt = []
        if start_date is None:
            start_date = '20000101'
        if end_date is None:
            end_date = '21001231'
        for iDay in Days:
            if iDay >= start_date and iDay <= end_date:
                Rslt.append(iDay)
        return Rslt

    def tradeDayOffset(self, today, n, freq='1d', first_or_last='L'):
        if isinstance(today, (pd.DatetimeIndex, pd.datetime)):
            today = Datetime2DateStr(today)
        if n < 0:
            tempData = self.get_trade_days(end_date=today, freq=freq, first_or_last=first_or_last)
            tempData.sort(reverse=True)
        else:
            tempData = self.get_trade_days(start_date=today, freq=freq, first_or_last=first_or_last)
        if today in tempData:
            return tempData[abs(n)]
        else:
            return tempData[abs(n) - 1]
    
    def is_trade_day(self,day):
        if isinstance(day,str):
            return day in trade_calendar.allTradeDays
        else:
            return day in pd.DatetimeIndex(trade_calendar.allTradeDays)

    def latest_trade_day(self, day, trade_days):
        """
        计算在trade_days中距离day最近的一天
        :param day:
        :param trade_days: DatetimeIndex
        :return: latest_day
        """
        if day >= max(trade_days):
            return day
        return trade_days[trade_days >= day][0]