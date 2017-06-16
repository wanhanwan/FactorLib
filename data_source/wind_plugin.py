# wind插件API
from WindPy import *
from data_source import data_api
from utils.tool_funcs import tradecode_to_windcode, windcode_to_tradecode
from const import MARKET_INDEX_WINDCODE
import pandas as pd
import xlrd
import os

current_path = os.path.abspath('.')
workbook_path = os.sep.join(current_path.split(os.sep)[:2] + ['resource', 'wind_addin.xlsx'])

argInfoWB = xlrd.open_workbook(workbook_path) 
argInfo = pd.read_excel(argInfoWB,sheetname='ArgInfo',engine='xlrd')

# 行情数据接口
def get_history_bar(field_names, start_date, end_date,id_type='stock', **kwargs):
    if not isinstance(field_names,list):
        field_names = [field_names]

    if id_type == 'stock':
        field_info = pd.read_excel(argInfoWB,sheetname='收盘行情',engine='xlrd')
         # 按照字段循环取数据
        _l = []
        w.start()
        for fieldName in field_names:
            field_name = field_info[field_info['FactorName']==fieldName]['FieldName'].iat[0]
            args = field_info[field_info['FactorName']==fieldName]['Args'].iat[0]
          
            params = _parse_args(eval(args),**kwargs)
            all_days = data_api.tc.get_trade_days(start_date, end_date)
            all_ids = data_api.get_history_ashare(all_days).index.levels[1].unique()
  
            data = w.wsd(
                list(map(tradecode_to_windcode, all_ids)), field_name, start_date, end_date, params)
            _l.append(_bar_to_dataframe(data))
        data = pd.concat(_l,axis=1)
        w.close()
    elif id_type == 'index':
        field_info = pd.read_excel(argInfoWB,sheetname='指数收盘行情',engine='xlrd')
        all_ids = [MARKET_INDEX_WINDCODE[x] for x in MARKET_INDEX_WINDCODE]
        _l = []
        w.start()
        for fieldName in field_names:
            field_name = field_info[field_info['FactorName']==fieldName]['FieldName'].iat[0]
            args = field_info[field_info['FactorName']==fieldName]['Args'].iat[0]
          
            params = _parse_args(eval(args),**kwargs)
            data = w.wsd(all_ids, field_name, start_date, end_date, params)
            _l.append(_bar_to_dataframe(data))
        data = pd.concat(_l,axis=1)
        w.close()
    return data


def _parse_args(args,**kwargs):
    """解析参数信息，返回WindAPI字符串"""
    arg_str = []
    for arg in args:
        arg_name_str = argInfo[argInfo['ArgName']==arg]['ArgNameStr'].iat[0]
        if arg not in kwargs:
            arg_value_str = argInfo[argInfo['ArgName']==arg]['DefaultValue'].iat[0]
        else:
            arg_value_str = argInfo[
            (argInfo['ArgName']==arg) & (argInfo['ArgValue']==kwargs[arg])]['ArgValueStr'].iat[0]
            if (arg_value_str == 'NaN') or pd.isnull(arg_value_str):
                continue
        arg_str.append(
            "{arg_name}={arg_value}".format(
                arg_name=arg_name_str, arg_value=arg_value_str)
            )
    return ";".join(arg_str)

def _bar_to_dataframe(data):
    """把windAPI数据转换成dataframe"""
    ids = list(map(windcode_to_tradecode, data.Codes))
    dates = [x.date() for x in data.Times]
    col = pd.Index(ids, name='IDs')
    if len(dates) == 1:
        df = pd.DataFrame(data.Data)
    else:
        df = pd.DataFrame(data.Data).T
    df.index = pd.DatetimeIndex(dates,name='date')
    df.columns = col
    df = df.stack().to_frame().sort_index().rename(columns={0:data.Fields[0].lower()})
    return df


