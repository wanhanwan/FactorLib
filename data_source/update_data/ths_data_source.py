import pandas as pd
from iFinDPy import *
from const import THS_USERID, THS_PASSWORD, DEFAULT_PARAMS, CS_INDUSTRY_CODES
from utils.tool_funcs import tradecode_to_windcode, drop_patch
from data_source import sec, tc, h5

THS_iFinDLogin(THS_USERID, THS_PASSWORD)


def _params2dict(params):
    _d = {}
    for key_value in params.split(","):
        key, value = key_value.split(":")
        _d[key] = value
    return _d

def _param2str(param_dict):
    _s = []
    for k, v in param_dict.items():
        _s.append("%s:%s"%(k, v))
    return ",".join(_s)

def _adjust_params(params, pricetype, period, kwargs):
    p_dict = _params2dict(params)
    p_dict['pricetype'] = pricetype
    p_dict['period'] = period
    p_dict.update(kwargs)
    return _param2str(p_dict)


def _updateHistoryBar(ids, start, end, fields, pricetype, period='D', **kwargs):
    if isinstance(fields, str):
        fields = [fields]
    params = _adjust_params(DEFAULT_PARAMS, pricetype, period, kwargs)
    all_dates = tc.get_trade_days(start, end)
    fields = ",".join(fields)

    _l = []
    for d in all_dates:
        # print(d)
        date_param = '-'.join([d[:4], d[4:6], d[-2:]])
        codes_params = ",".join(ids)
        data = THS_HistoryQuotes(codes_params, fields, params, date_param, date_param)
        data_frame = THS_Trans2DataFrame(data)
        _l.append(data_frame)
    data = pd.concat(_l)
    data['IDs'] = data['thscode'].apply(drop_patch)
    data.rename(columns={'time': 'date'}, inplace=True)
    data['date'] = pd.DatetimeIndex(data['date'])
    data.set_index(['date', 'IDs'], inplace=True)
    data = data[pd.notnull(data.index.get_level_values(0))]
    return data

if __name__ == '__main__':
    # 中信一级行业的历史行情数据
    fields = ['open', 'high', 'low', 'close', 'changeper', 'volume']
    data = _updateHistoryBar(CS_INDUSTRY_CODES, '20160405', '20160405', fields, 1)
    h5.save_factor(data, '/indexprices/cs_level_1/')