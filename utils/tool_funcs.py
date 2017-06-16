"""一些工具函数"""
from const import INDUSTRY_NAME_DICT, SW_INDUSTRY_DICT, CS_INDUSTRY_DICT
import pandas as pd
import os

def dict_reverse(_dict):
    return {_dict[x]:x for x in _dict}

def parse_industry(industry):
    if industry[:2] == '中信':
        return INDUSTRY_NAME_DICT['中信一级']
    else:
        return INDUSTRY_NAME_DICT['申万一级']

def anti_parse_industry(industry):
    return dict_reverse(INDUSTRY_NAME_DICT)[industry]

def write_df_to_excel(sheet,start_point,df,index=True,columns=True):
    if index:
        df = df.reset_index()
    df_shape = df.shape
    if columns:
        for i,x in enumerate(df.columns):
            _ = sheet.cell(column=start_point[1]+i,row=start_point[0],
                           value=x)
        start_point = (start_point[0]+1,start_point[1])
    for r in range(df_shape[0]):
        for c in range(df_shape[1]):
            col = start_point[1] + c
            row = start_point[0] + r
            _ = sheet.cell(column=col,row=row,value=df.iloc[r,c])
    end_point = (start_point[0]+ df_shape[0]-1,start_point[1]+ df_shape[1]-1)
    return end_point

def tradecode_to_windcode(tradecode):
    return tradecode + '.SH' if tradecode[0] == '6' else tradecode + '.SZ'
        
def windcode_to_tradecode(windcode):
    return windcode[:6]

def import_mod(mod_name):
    try:
        from importlib import import_module
        return import_module(mod_name)
    except Exception as e:
        return None

def ensure_dir_exists(dir_path):
    import os
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    return dir_path

def get_industry_names(industry_symbol, industry_info):
    if industry_symbol == 'sw_level_1':
        series = pd.Series(SW_INDUSTRY_DICT).to_frame().rename(columns={0:industry_symbol})
        series.index = series.index.astype("int32")
    elif industry_symbol == 'cs_level_1':
        series = pd.Series(CS_INDUSTRY_DICT).to_frame().rename(columns={0: industry_symbol})
        series.index = [int(x[2:]) for x in series.index]
    elif industry_symbol == 'cs_level_2':
        level_2_excel = os.path.abspath(os.path.abspath("..") +'/..') + os.sep + "resource" + os.sep + "level_2_industry_dict.xlsx"
        level_2_dict = pd.read_excel(level_2_excel, sheetname=industry_symbol, header=0)
        level_2_dict['Code'] = level_2_dict['Code'].apply(lambda x: int(x[2:]))
        series = level_2_dict.set_index('Code').rename(columns={'Name': industry_symbol})
    elif industry_symbol == 'sw_level_2':
        level_2_excel = os.path.abspath(os.path.abspath("..") +'/..') + os.sep + "resource" + os.sep + "level_2_industry_dict.xlsx"
        level_2_dict = pd.read_excel(level_2_excel, sheetname=industry_symbol, header=0)
        level_2_dict['Code'] = level_2_dict['Code'].apply(int)
        series = level_2_dict.set_index('Code').rename(columns={'Name': industry_symbol})        
    industry_info.columns = ['industry_code']
    return industry_info.join(series, on='industry_code', how='left')[[industry_symbol]]

def get_industry_code(industry_symbol, industry_info):
    level_2_excel = os.path.abspath(os.path.abspath("..") +'/..') + os.sep + "resource" + os.sep + "level_2_industry_dict.xlsx"
    level_2_dict = pd.read_excel(level_2_excel, sheetname=industry_symbol, header=0)
    industry_info.columns = ['industry_code']
    if industry_symbol == 'cs_level_2':
        level_2_dict['Code'] = level_2_dict['Code'].apply(lambda x: int(x[2:]))
        series = level_2_dict.set_index('Name').rename(columns={'Code': industry_symbol})
        return industry_info.join(series, on='industry_code', how='left')[[industry_symbol]]
    elif industry_symbol == 'sw_level_2':
        level_2_dict['Code'] = level_2_dict['Code'].apply(int)
        series = level_2_dict.set_index('Name').rename(columns={'Code': industry_symbol})
        temp = industry_info.join(series, on='industry_code', how='left')[[industry_symbol]]
        temp = temp.unstack().fillna(method='backfill').stack().astype('int32')
        return temp

# 将某报告期回溯N期
def RollBackNPeriod(report_date,n_period):
    Date = report_date
    for i in range(1,n_period+1):
        if Date[-4:]=='1231':
            Date = Date[0:4]+'0930'
        elif Date[-4:]=='0930':
            Date = Date[0:4]+'0630'
        elif Date[-4:]=='0630':
            Date = Date[0:4]+'0331'
        elif Date[-4:]=='0331':
            Date = str(int(Date[0:4])-1)+'1231'
    return Date