import pandas as pd
import sys
sys.path.append("..")
from datetime import datetime
from wind_db import WindDB
from local_db import LocalDB

db = WindDB()
db.connectDB()
local = LocalDB()
local.connectDB()


def update_hqdata(start, end):
    print("正在更新历史行情数据...")
    # maxdate = local.getMaxDate('hq_data')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = maxdate, 1)
    sql = """SELECT
        SUBSTR (T .s_info_windcode, 1, 6),
        T .trade_dt,
        T .s_dq_close,
        T .s_dq_adjclose,
        T .s_dq_pctchange / 100, 
        E .s_val_mv
    FROM
        ASHAREEODPRICES T
    LEFT JOIN ASHAREEODDERIVATIVEINDICATOR E ON T .s_info_windcode = E .s_info_windcode
    AND T .trade_dt = E .trade_dt
    WHERE
        T .trade_dt BETWEEN '%s'
    AND '%s'
    ORDER BY
        T .TRADE_DT,
        T .S_INFO_WINDCODE
    """%(start, end)
    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','spj','adjclose','rsyl','zsz']
    data = cursor.fetchall()
    cursor.close()
    if data:
        local.insertRowsWithID('hq_data', columns, data)
    print("历史行情数据更新成功....")
    return 1

def update_jyzt(start, end):
    print("正在更新交易状态数据...")
    # maxdate = local.getMaxDate('jyzt')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = maxdate, 1)
    sql = """
        SELECT substr(s_info_windcode,1,6),
                     trade_dt,
                     s_dq_volume,
                     s_dq_pctchange,
               s_dq_high,
               s_dq_low
        from ASHAREEODPRICES
        WHERE TRADE_DT BETWEEN '%s' and '%s'
        ORDER BY S_INFO_WINDCODE,TRADE_DT
    """%(start, end)

    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','cjl','zdf','zgj','zdj']
    data = cursor.fetchall()
    cursor.close()
    
    if data:
        local.insertRowsWithID('jyzt', columns, data)
    print("交易状态更新成功....")
    return 1


def idx_weight_hs300(start, end):
    print("正在更新沪深300指数权重...")
    # maxdate = local.getMaxDate('idx_weight_hs300')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = maxdate, 1)
    sql = """
        SELECT SUBSTR(S_CON_WINDCODE,1,6),
               TRADE_DT,
               I_WEIGHT/100 
        from AINDEXHS300FREEWEIGHT t WHERE t.S_INFO_WINDCODE='399300.SZ' 
        AND TRADE_DT BETWEEN '%s' and '%s' ORDER BY TRADE_DT
    """%(start, end)
    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','Weight']
    data = cursor.fetchall()
    cursor.close()
    
    if data:
        local.insertRowsWithID('idx_weight_hs300', columns, data)
    print("沪深300指数权重更新成功....")
    return 1

def idx_weight_zz500(start, end):
    print("正在更新中证500指数权重...")
    # maxdate = local.getMaxDate('idx_weight_zz500')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = maxdate, 1)
    sql = """
        SELECT SUBSTR(S_CON_WINDCODE,1,6),
               TRADE_DT,
               I_WEIGHT/100 
        from AINDEXHS300FREEWEIGHT t WHERE t.S_INFO_WINDCODE='000905.SH' 
        AND TRADE_DT BETWEEN '%s' and '%s' ORDER BY TRADE_DT
    """%(start, end)
    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','Weight']
    data = cursor.fetchall()
    cursor.close()
    if data:
        local.insertRowsWithID('idx_weight_zz500', columns, data)
    print("中证500指数权重更新成功....")
    return 1


def float_mv(start, end):
    print("正在更新流通市值数据...")
    # maxdate = local.getMaxDate('float_mkt_value')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = tradeDayOffset(maxdate, 1)
    sql = """
        SELECT
            SUBSTR (s_info_windcode, 1, 6),
            trade_dt,
            s_dq_mv
        FROM
            ASHAREEODDERIVATIVEINDICATOR
        WHERE
            TRADE_DT BETWEEN '%s'
        AND '%s'
        ORDER BY
            S_INFO_WINDCODE,
            TRADE_DT
    """%(start, end)

    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','float_mv']
    data = cursor.fetchall()
    cursor.close()

    if data:
        local.insertRowsWithID('float_mkt_value', columns, data)
    print("流通市值更新成功....")
    return 1


def pb_pe_pcf(start, end):
    print("正在更新估值类数据...")
    # maxdate = local.getMaxDate('pe_pb_pcf')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = tradeDayOffset(maxdate, 1)
    sql = """
        SELECT substr(s_info_windcode,1,6),
                     trade_dt,
                     s_val_pb_new,
                   s_val_pe,
               s_val_pcf_ocf,
               s_val_pe_ttm
        FROM ASHAREEODDERIVATIVEINDICATOR
        WHERE TRADE_DT BETWEEN %s and %s ORDER BY S_INFO_WINDCODE,TRADE_DT
    """%(start,end)
    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','pb','pe','pcf','pe_ttm']
    data = pd.DataFrame(cursor.fetchall())
    cursor.close()
    
    if not data.empty:
        data = data.where(pd.notnull(data), other='NULL')
        local.insertRowsWithID('pe_pb_pcf', columns, data.values)
    print("估值类数据更新成功...")
    return 1


def index_price(start, end):
    print("正在更新指数行情数据...")
    # maxdate = local.getMaxDate('index_price')
    # if maxdate is None:
    #     maxdate = '20170301'
    # if maxdate >= end:
    #     return 0
    # start = tradeDayOffset(maxdate, 1)
    Index_WindCode = ['000001.SH','399985.SZ','000300.SH','000905.SH','000906.SH','399102.SZ']
    
    sql = """select substr(s_info_windcode,1,6),
                    trade_dt,
                    s_dq_open,
                    s_dq_high,
                    s_dq_low,
                    s_dq_close,
                    s_dq_volume,
                    s_dq_amount 
             from aindexeodprices 
             where s_info_windcode in ('"""
    sql += "','".join(Index_WindCode)+"') and trade_dt between "+start+" and "+end
    cursor = db.execSQLQuery(sql)
    data = cursor.fetchall()
    cursor.close()
    columns = ['IDs','Dates','open','high','low','close','vol','amt']
    
    if data:
        local.insertRowsWithID('index_price', columns, data)
    print("指数行情更新成功...")
    return 1

def turnover(start,end):
    print("正在更新换手率数据...")
    # maxdate = local.getMaxDate('turnover')
    # if maxdate is None:
    #     maxdate = '20170201'
    # if maxdate >= end:
    #     return 0
    # start = tradeDayOffset(maxdate, 1)
    sql = """
        SELECT substr(s_info_windcode,1,6),
                     trade_dt,
                     s_dq_turn,
                   s_dq_freeturnover
        FROM ASHAREEODDERIVATIVEINDICATOR
        WHERE TRADE_DT BETWEEN %s and %s ORDER BY S_INFO_WINDCODE,TRADE_DT
    """%(start,end)
    cursor = db.execSQLQuery(sql)
    columns = ['IDs','Dates','turn','free_turn']
    data = pd.DataFrame(cursor.fetchall())
    cursor.close()
    
    if not data.empty:
        data = data.where(pd.notnull(data), other='NULL')
        local.insertRowsWithID('turnover', columns, data.values)
    print("换手率更新成功...")
    return 1

# updateFuncs = [update_hqdata, update_jyzt, idx_weight_hs300, idx_weight_zz500,
#                float_mv, pb_pe_pcf, index_price,turnover]

updateFuncs = [idx_weight_hs300,idx_weight_zz500,pe_pb_pcf]


