"""一些工具函数"""
from const import INDUSTRY_NAME_DICT

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