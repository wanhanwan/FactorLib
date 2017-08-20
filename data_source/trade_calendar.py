import os
import warnings
import pandas as pd
from pandas.tseries.frequencies import to_offset
from pandas.tseries.offsets import (DateOffset,
                                    CustomBusinessDay,
                                    CustomBusinessMonthEnd,
                                    Week,
                                    QuarterEnd,
                                    YearEnd,
                                    QuarterOffset,
                                    YearOffset,
                                    apply_wraps,
                                    _is_normalized,
                                    as_timestamp
                                    )
from functools import wraps
from collections import Iterable
from datetime import time, timedelta


_default_min_date = as_timestamp('20000101')
_default_max_date = as_timestamp('20201231')


def _read_holidays():
    root_dir = os.path.abspath(__file__+'/../..')
    csv_path = os.path.join(root_dir, 'resource/trade_dates.csv')
    allTradeDays = pd.read_csv(csv_path, index_col=0, header=None, squeeze=True, dtype='str').values.tolist()
    allTradeDays_idx = pd.DatetimeIndex(allTradeDays)
    all_dates = pd.date_range(min(allTradeDays), max(allTradeDays))
    holidays = [x for x in all_dates if (x not in allTradeDays_idx) and (x.weekday() not in [5, 6])]
    return holidays
chn_holidays = _read_holidays()


class CustomBusinessWeekEnd(DateOffset):
    _cacheable = False
    _prefix = 'CBWE'

    def __init__(self, n=1, normalize=False, weekmask='Mon Tue Wed Thu Fri',
                 holidays=None, calendar=None, **kwds):
        self.n = n
        self.normalized = normalize
        self.kwds = kwds
        self.offset = kwds.get('offset', timedelta(0))
        self.cbday = CustomBusinessDay(n=1, normalize=normalize, weekmask=weekmask, holidays=holidays,
                                       calendar=calendar, **kwds)
        self.kwds['calendar'] = self.cbday.calendar
        self.w_offset = Week(weekday=4)

    @apply_wraps
    def apply(self, other):
        n = self.n
        result = other
        if n == 0:
            n = 1
        if n > 0:
            while result <= other:
                next_fri = other + n * self.w_offset
                result = self.cbday.rollback(next_fri)
                n += 1
        else:
            while result >= other:
                last_fri = other + n * self.w_offset
                result = self.cbday.rollback(last_fri)
                n -= 1
        return result

    def onOffset(self, dt):
        if self.normalize and not _is_normalized(dt):
            return False
        if not self.cbday.onOffset(dt):
            return False
        return (dt + self.cbday).week != dt.week


class CustomBusinessQuaterEnd(QuarterOffset):
    _cacheable = False
    _prefix = 'CBQE'

    def __init__(self, n=1, normalize=False, weekmask='Mon Tue Wed Thu Fri',
                 holidays=None, calendar=None, **kwds):
        self.n = n
        self.normalize = normalize
        self.kwds = kwds
        self.offset = kwds.get('offset', timedelta(0))
        self.startingMonth = kwds.get('startingMonth', 3)
        self.cbday = CustomBusinessDay(n=1, normalize=normalize, weekmask=weekmask, holidays=holidays,
                                       calendar=calendar, **kwds)
        self.kwds['calendar'] = self.cbday.calendar
        self.q_offset = QuarterEnd(1)

    @apply_wraps
    def apply(self, other):
        n = self.n
        cur_qend = self.q_offset.rollforward(other)
        cur_cqend = self.cbday.rollback(cur_qend)

        if n == 0 and other != cur_cqend:
            n += 1
        if other < cur_cqend and n >= 1:
            n -= 1
        if other > cur_cqend and n <= -1:
            n += 1

        new = cur_qend + n * self.q_offset
        result = self.cbday.rollback(new)
        return result

    def onOffset(self, dt):
        if self.normalize and not _is_normalized(dt):
            return False
        if not self.cbday.onOffset(dt):
            return False
        return (dt + self.cbday).quater != dt.quater


class CustomBusinessYearEnd(YearOffset):
    _cacheable = False
    _prefix = 'CBYE'

    def __init__(self, n=1, normalize=False, weekmask='Mon Tue Wed Thu Fri',
                 holidays=None, calendar=None, **kwds):
        self.n = n
        self.normalize = normalize
        self.kwds = kwds
        self.offset = kwds.get('offset', timedelta(0))
        self.cbday = CustomBusinessDay(n=1, normalize=normalize, weekmask=weekmask, holidays=holidays,
                                       calendar=calendar, **kwds)
        self.kwds['calendar'] = self.cbday.calendar
        self.y_offset = YearEnd(1)

    @apply_wraps
    def apply(self, other):
        n = self.n
        cur_yend = self.y_offset.rollforward(other)
        cur_cyend = self.cbday.rollback(cur_yend)

        if n == 0 and other != cur_cyend:
            n += 1
        if other < cur_cyend and n >= 1:
            n -= 1
        if other > cur_cyend and n <= -1:
            n += 1

        new = cur_yend + n * self.y_offset
        result = self.cbday.rollback(new)
        return result

    def onOffset(self, dt):
        if self.normalize and not _is_normalized(dt):
            return False
        if not self.cbday.onOffset(dt):
            return False
        return (dt + self.cbday).year != dt.year

traderule_alias_mapping = {
    'd': CustomBusinessDay(holidays=chn_holidays),
    'w': CustomBusinessWeekEnd(holidays=chn_holidays),
    'm': CustomBusinessMonthEnd(holidays=chn_holidays),
    'q': CustomBusinessQuaterEnd(holidays=chn_holidays),
    'y': CustomBusinessYearEnd(holidays=chn_holidays)
}


def _to_offset(freq):
    if freq[1] in traderule_alias_mapping:
        return traderule_alias_mapping.get(freq[1]) * int(freq[0])
    else:
        return to_offset(freq)


def _validate_date_range(start, end):
    start = _default_min_date if start is None else as_timestamp(start)
    end = _default_max_date if end is None else as_timestamp(end)
    return max(start, _default_min_date), min(end, _default_max_date)


def handle_retstr(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if 'retstr' not in kwargs:
            retstr = '%Y%m%d'
        elif kwargs['retstr'] is None:
            return func(self, *args, **kwargs)
        else:
            retstr = kwargs['retstr']
        result = func(self, *args, **kwargs).strftime(retstr)
        try:
            return result.tolist()
        except:
            return result
    return wrapper


class trade_calendar(object):

    @handle_retstr
    def get_trade_days(self, start_date=None, end_date=None, freq='1d', **kwargs):
        """
        获得日期序列，支持日历日和交易日。

        freq支持自定义格式和Pandas自带的DateOffset格式。当freq是字符串时，它由两部分构成。第一个字符为数字
        表示间隔数目,后面字母表示频率,目前交易日频率包括d、w、m、q、y(日、周、月、季、年)。

        日期序列的范围是2000-01-01至2020-12-31
        """
        start_date, end_date = _validate_date_range(start_date, end_date)
        offset = _to_offset(freq)
        result = pd.date_range(start_date, end_date, freq=offset)
        return result

    @handle_retstr
    def tradeDayOffset(self, today, n, freq='1d', incl_on_offset_today=False, **kwargs):
        """
        日期漂移

        若参数n为正，返回以today为起始日向前推第n个交易日，反之亦然。
        若n为零，返回以today为起点，向后推1个freq的交易日。

        注意:
            若incl_on_offset_today=True，today on offset时，漂移的起点是today，today not
            offset时，漂移的起点是today +- offset
            若incl_on_offset_today=False，日期漂移的起点是today +- offset。

        例如：
            2017-08-18是交易日, 2017-08-20不是交易日，则：

            tradeDayOffset('2017-08-18', 1, freq='1d', incl_on_offset_today=False) -> 2017-08-21

            tradeDayOffset('2017-08-18', 1, freq='1d', incl_on_offset_today=True) -> 2017-08-18

            tradeDayOffset('2017-08-18', 2, freq='1d', incl_on_offset_today=True) -> 2017-08-21

            tradeDayOffset('2017-08-18', -1, freq='1d', incl_on_offset_today=True) -> 2017-08-18

            tradeDayOffset('2017-08-18', -2, freq='1d', incl_on_offset_today=True) -> 2017-08-17

            tradeDayOffset('2017-08-18', 0, freq='1d', incl_on_offset_today=False) -> 2017-08-18

            tradeDayOffset('2017-08-20', 0, freq='1d', incl_on_offset_today=True) -> 2017-08-18
        """

        today = as_timestamp(today)
        if n == 0:
            if int(freq[0]) > 1:
                warnings.warn('invalid step length of freq. It must be 1 when n=0')
                n = -1 * (int(freq[0]))
                freq = "1%s" % freq[1]
            else:
                return traderule_alias_mapping[freq[1]].rollback(today)
        offset = _to_offset(freq)
        if incl_on_offset_today and offset.onOffset(today):
            n = int((abs(n) - 1) * (n / abs(n)))
        return today + n * offset

    @staticmethod
    def is_trade_day(day, freq='1d'):
        """
        交易日判断。
        """
        time_stamp = as_timestamp(day)
        return _to_offset(freq).onOffset(time_stamp)

    @handle_retstr
    def get_latest_trade_days(self, days, **kwargs):
        """
        遍历days中的每个元素，返回距离每个元素最近的交易日。
        """
        if isinstance(days, Iterable):
            timeindex = pd.DatetimeIndex(days)
            return pd.DatetimeIndex([traderule_alias_mapping['d'].rollback(x) for x in timeindex])
        else:
            return traderule_alias_mapping['d'].rollback(as_timestamp(days))

    @staticmethod
    def is_trading_time(self, date_time):
        """
        交易时间判断

        """
        is_tradingdate = self.is_trade_day(date_time.date())
        is_tradingtime = time(9, 25, 0) < date_time.time() < time(15, 0, 0)
        return is_tradingdate and is_tradingtime

tc = trade_calendar()
pd.TimeGrouper
