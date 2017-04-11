from multiprocessing import Lock
import os
import gc
import shutil
from contextlib import closing
from database.AuxiliaryFun import (getCurrentDateStr,
                                   genAvailableName,
                                   Datetime2DateStr,
                                   DictKeyValueTurn,
                                   CodeConvert,
                                   CodeConvertStr,
                                   CodeConvertWind)
import shelve
import itertools
import re

import pandas as pd
import numpy as np
import scipy.io as sio
import sqlite3

sqliteDB="D:/data/sqlLiteDB"
ShelveDBPath = "D:/data/ShelveDB" 
class WindDB(object):
    """Wind Old Data Base"""

    def __init__(self, user="windfile", password="windfile", host="172.20.65.11", port="1521", dbname="wind",
                 data_lock=Lock(), wind_dict_path=None,
                 db_type='Oracle', lib_path=None, table_prefix='wind.'):
        self.User = user
        self.Password = password
        self.Host = host
        self.Port = port
        self.DBName = dbname
        self.Connection = None
        self.WindDictPath = wind_dict_path
        self.LibPath = lib_path
        self.Data = data_lock
        self.DBType = db_type
        self.TablePrefix = table_prefix
        if self.WindDictPath is not None:
            self.DataLock.acquire()
            DictFile = shelve.open(self.WindDictPath)
            self.TableInfo = DictFile['TableInfo']
            self.FactorInfo = DictFile['FactorInfo']
            DictFile.close()
            self.DataLock.release()
        return

    # -------------------------------------------数据库的连接与查询------------------------------------
    def getConnInfo(self):
        return {'User': self.User, 'Password': self.Password, 'Host': self.Host, 'Port': self.Port,
                'DBName': self.DBName, 'DBType': self.DBType}

    # 连接数据库
    def connectDB(self):
        if self.DBType == 'Oracle':
            try:
                import cx_Oracle
                self.Connection = cx_Oracle.connect(self.User, self.Password,
                                                    cx_Oracle.makedsn(self.Host, self.Port, self.DBName))
                return 1
            except Exception as e:
                print(e)
                pass
        elif self.DBType == 'SQL Server':
            try:
                import pymssql
                self.Connection = pymssql.connect(server=self.Host, user=self.User, password=self.Password,
                                                  database=self.DBName, charset='UTF-8', port=self.Port)
                return 1
            except:
                pass
        try:
            import pyodbc
            ConnInfo = 'DRIVER={' + self.DBType + '};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (
            self.DBName, self.Host, self.User, self.Password)
            self.Connection = pyodbc.connect(ConnInfo)
            return 1
        except:
            pass
        return -1

    # 断开数据库
    def disconnectDB(self):
        try:
            if self.Connection is not None:
                self.Connection.close()
                self.Connection = None
            return 1
        except:
            return -1

    # 获取数据库的连接
    def getConnection(self):
        return self.Connection

    # 检查数据库是否可用
    def isDBAvailable(self):
        if self.Connection is None:
            return False
        else:
            return True

    # 执行SQL查询语句
    def execSQLQuery(self, sql_str):
        if self.Connection is not None:
            Cursor = self.Connection.cursor()
            Cursor.execute(sql_str)
            return Cursor
        else:
            return None

    def __str__(self):
        ObjectStr = 'Host : %s \n Port : %s \n Data base name : %s \n User ID : %s \n Password : %s' % (
        self.Host, self.Port, self.DBName, self.User, self.Password)
        return ObjectStr


class LocalDB(object):
    """Wind Old Data Base"""

    def __init__(self, user="root", password="123456", host='172.20.95.168', dbname="barrafactors"):
        self.user = user
        self.password = password
        self.dbname = dbname
        self.host = host

    def connectDB(self):
        import pymysql
        try:
            self.Connection = pymysql.connect(user=self.user, passwd=self.password, host=self.host, db=self.dbname)
            return 1
        except pymysql.Error as e:
            print(e)
            return -1

    def disconnectDB(self):
        try:
            if self.Connection is not None:
                self.Connection.close()
                self.Connection = None
            return 1
        except:
            return -1

    # 获取数据库的连接
    def getConnection(self):
        return self.Connection

    # 检查数据库是否可用
    def isDBAvailabe(self):
        if self.Connection is None:
            return False
        else:
            return True

    # 执行SQL查询语句, 返回游标
    def execSQLQuery(self, sql_str, multi=False):
        if self.Connection is not None:
            Cursor = self.Connection.cursor()
            if multi:
                Cursor.execute(sql_str, multi=True)
            else:
                Cursor.execute(sql_str)
            return Cursor
        else:
            return None

    #获取某张表的因子数据
    def loadFactor(self,table,factors=None,
                   ids=None,dates=None,start_date=None,
                   end_date=None,date_col='trade_dt'):

        if factors is None:
            sql_query_cols = 'show columns from ' + table
            cur = self.execSQLQuery(sql_query_cols)
            factors = [x[0] for x in cur.fetchall()]
            cur.close()   
        sql_query="SELECT "+",".join(factors)+" FROM "+table
        if ids is not None:
            ids = CodeConvert(CodeConvertWind(ids))            
            sql_query+=" WHERE windcode IN ('"+"','".join(ids)+"')"
        if dates is not None:
            if ids is None:
                sql_query+=" WHERE %s IN ("%date_col+",".join(dates)+")"
            else:
                sql_query+=" AND %s IN ("%date_col+",".join(dates)+")"
        elif (start_date is not None) and (end_date is not None):
            if ids is not None:
                sql_query+=" AND %s BETWEEN %s AND %s"%(date_col,start_date,end_date)
            else:
                sql_query+=" WHERE %s BETWEEN %s AND %s"%(date_col,start_date,end_date)
        elif (start_date is None) and (end_date is not None):
            if ids is not None:
                sql_query+=" AND %s <= %s"%(date_col,end_date)
            else:
                sql_query+=" WHERE %s <= %s"%(date_col,end_date)
        elif (start_date is not None) and (end_date is None):
            if ids is not None:
                sql_query+=" AND %s >= %s"%(date_col,start_date)
            else:
                sql_query+=" WHERE %s >= %s"%(date_col,start_date)
        cursor = self.execSQLQuery(sql_query)
        rslt = pd.DataFrame(list(cursor.fetchall()), columns=factors)
        cursor.close()
        rslt = format_column_names(rslt)
        rslt = rslt[rslt['IDs'] < 900000000]
        rslt['IDs'] = rslt['IDs'].astype('str').str[3:]
        rslt['Date'] = rslt['Date'].astype('str')
        return rslt

    def insertRow(self, tablename, col, value, transaction=0):
        valuesStr = map(str, value)
        valuesStr = [i for i in valuesStr]

        sql = "INSERT INTO " + tablename + " (" + ','.join(col) + ") VALUES (" + ','.join(valuesStr) + ");"
        # print(sql)
        if self.Connection is not None:
            cursor = self.Connection.cursor()
            cursor.execute(sql)
            cursor.close()
            if transaction == 0:
                self.Connection.commit()
            return 1

    # 批量插入记录
    def insertRows(self, tablename, col, value, transaction=0):
        # value 为二维列表
        sql = "insert into " + tablename + "(" + ','.join(col) + ") values"
        numRows = len(value)
        if self.Connection is not None:
            cursor = self.Connection.cursor()
            for i in range(numRows):
                if len(sql) >= 10485760:
                    sql = sql[:-1]
                    cursor.execute(sql)
                    sql = "insert into " + tablename + "(" + ','.join(col) + ") values"
                valuesStr = map(str, value[i])
                valuesStr = [i for i in valuesStr]
                #                    valuesStr[0] = "'"+valuesStr[0]+"'"
                if i < numRows - 1:
                    sql = sql + "(" + ','.join(valuesStr) + "),"
                else:
                    sql = sql + "(" + ','.join(valuesStr) + ")"
            cursor.execute(sql)
            cursor.close()
            if transaction == 0:
                self.Connection.commit()
            return 1

    # 带自增id的批量插入记录
    def insertRowsWithID(self, tablename, col, value, transaction=0):
        # value 为二维列表
        sql = "insert into " + tablename + "(" + ','.join(col) + ") values"
        numRows = len(value)
        if self.Connection is not None:
            cursor = self.Connection.cursor()
            for i in range(numRows):
                if len(sql) >= 10485760:
                    sql = sql[:-1] + "on duplicate key update id=id"
                    cursor.execute(sql)
                    sql = "insert into " + tablename + "(" + ','.join(col) + ") values"
                valuesStr = map(str, value[i])
                valuesStr = [i for i in valuesStr]
                #                    valuesStr[0] = "'"+valuesStr[0]+"'"
                if i < numRows - 1:
                    sql = sql + "(" + ','.join(valuesStr) + "),"
                else:
                    sql = sql + "(" + ','.join(valuesStr) + ") on duplicate key update id=id"
            cursor.execute(sql)
            cursor.close()
            if transaction == 0:
                self.Connection.commit()
            return 1

    # --------------------表的管理------------------------
    # 获取barrafactors数据库中的表名
    def getTableName(self, dbname='barrafactors', ignore=[]):
        SQLStr = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\'' + dbname + '\''
        Cursor = self.Connection.cursor()
        Cursor.execute(SQLStr)
        Result = Cursor.fetchall()
        Cursor.close()
        return [iRes[0] for iRes in Result if iRes[0] not in ignore]

    # 检查表是否存在

    def checkTableExistence(self, table_name, dbname='barrafactors'):
        SQLStr = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\'' + dbname + '\' AND TABLE_NAME=\'' + table_name + '\''
        Cursor = self.execSQLQuery(SQLStr)
        Result = Cursor.fetchall()
        Cursor.close()
        if len(Result) > 0:
            return True
        else:
            return False

    # 创建表,field_name_types字典{字段名:数据类型}，key=字段名，value=字段的数据类型,if_exists='cancel'：取消，'replace'：删除后重建,'error'：报错
    def createTable(self, table_name, field_name_types, if_exists='cancel'):
        SQLStr = 'CREATE TABLE %s (' % (table_name)
        for FieldName in field_name_types:
            SQLStr = SQLStr + '`%s` %s, ' % (FieldName, field_name_types[FieldName])
        SQLStr = SQLStr[:-2] + ')'
        Cursor = self.Connection.cursor()
        try:
            Cursor.execute(SQLStr)
            self.Connection.commit()
        except Exception as e:
            if if_exists == 'cancel':
                pass
            elif if_exists == 'replace':
                self.deleteTable(table_name)
                Cursor.execute(SQLStr)
                self.Connection.commit()
            elif if_exists == 'error':
                raise e
        Cursor.close()
        return 1

    # 重命名表
    def renameTable(self, old_table_name, new_table_name, dbname='barrafactors'):
        SQLStr = 'ALTER TABLE ' + dbname + '.' + old_table_name + ' RENAME TO  ' + dbname + '.' + new_table_name
        Cursor = self.Connection.cursor()
        Cursor.execute(SQLStr)
        self.Connection.commit()
        Cursor.close()
        return 1

    # 删除表
    def deleteTable(self, table_name):
        SQLStr = 'DROP TABLE %s' % (table_name)
        Cursor = self.Connection.cursor()
        try:
            Cursor.execute(SQLStr)
            self.Connection.commit()
        except:
            pass
        Cursor.close()
        return 1

    # 增加新字段
    def addNewFeilds(self, table_name, field_name_types, if_exists='cancel'):
        SQLStr = 'ALTER TABLE ' + table_name + ' ADD COLUMN '
        for FieldName in field_name_types:
            SQLStr = SQLStr + '%s %s,' % (FieldName, field_name_types[FieldName])
        SQLStr = SQLStr[:-2]

        Cursor = self.Connection.cursor()
        try:
            Cursor.execute(SQLStr)
            self.Connection.commit()
        except Exception as e:
            if if_exists == 'cancel':
                pass
            elif if_exists == 'replace':
                self.deleteFields(tablename, [FieldName for FieldName in field_name_types])
                Cursor.execute(SQLStr)
                self.Connection.commit()
            else:
                raise e
        Cursor.close()
        return 1

    # 删除字段
    def deleteFields(self, tablename, FieldName_List):
        SQLStr = 'ALTER TABLE %s DROP COLUMN ' % tablename
        if False in map(lambda x: isinstance(x, basestring), FieldName_List):
            raise Exception('The type of the elements in FieldName_List must be String!!')
        SQLStr = SQLStr + ','.join(FieldName_List)
        Cursor = self.Connection.cursor()
        try:
            Cursor.execute(SQLStr)
            self.Connection.commit()
        except:
            pass
        Cursor.close()
        return 1
    # 获取指定日当前或历史上的全体A股ID，返回在市场上出现过的所有A股
    def getAllAStock(self,date=getCurrentDateStr(),is_current=True,output_mode='list'):
        if is_current:
            SQLStr = 'SELECT s_info_code FROM '+self.TablePrefix+'AShareDescription WHERE (s_info_delistdate is NULL OR s_info_delistdate>\''+date+'\') AND s_info_listdate<=\''+date+'\' ORDER BY s_info_code'
        else:
            SQLStr = 'SELECT s_info_code FROM '+self.TablePrefix+'AShareDescription WHERE s_info_listdate<=\''+date+'\' ORDER BY s_info_code'
        if output_mode == 'SQL String':
            return SQLStr[:-18]
        Cursor = self.execSQLQuery(SQLStr)
        Result = Cursor.fetchall()
        if output_mode == 'list':
            return [rslt[0] for rslt in Result]
    # 获取行业名称和Wind内部查询代码
    def getIndustryWindID(self,industry_class_name='中信行业',level=1):
        LibFile = shelve.open(self.LibPath+os.sep+"WindIndustryCode")
        if industry_class_name=='中信一级行业':
            IndustryWindID = LibFile['中信行业1']
        elif industry_class_name=='申万一级行业':
            IndustryWindID = LibFile['申万行业1']
        elif industry_class_name=='Wind一级行业':
            IndustryWindID = LibFile['Wind行业1']
        else:
            IndustryWindID = LibFile.get(industry_class_name+str(level))
        if IndustryWindID is not None:
            return DictKeyValueTurn(dict(IndustryWindID))
        else:
            return None
    # 获取所有指数的名称和代码,((名称)，(代码))
    def getAllIndexNameID(self):
        SQLStr = 'SELECT '+self.TablePrefix+'AIndexDescription.s_info_name AS IndexName,'+self.TablePrefix+'AIndexDescription.s_info_code AS ID FROM '+self.TablePrefix+'AIndexDescription'
        SQLStr += ' ORDER BY '+self.TablePrefix+'AIndexDescription.s_info_code'
        Cursor = self.execSQLQuery(SQLStr)
        Res = Cursor.fetchall()
        Cursor.close()
        return tuple(zip(*Res))
    # 获取指数名称，ID，类别，风格
    def getIndexInfo(self):
        SQLStr = 'SELECT '+self.TablePrefix+'AIndexDescription.s_info_code as ID,'+self.TablePrefix+'AIndexDescription.s_info_compname as 指数名称,'+self.TablePrefix+'AIndexDescription.s_info_indexstyle as 指数类型,'+self.TablePrefix+'AIndexDescription.s_info_publisher as 交易所'
        SQLStr += ' FROM '+self.TablePrefix+'AIndexDescription'
        SQLStr += ' ORDER BY '+self.TablePrefix+'AIndexDescription.s_info_code'
        Cursor = self.execSQLQuery(SQLStr)
        Rslt = Cursor.fetchall()
        Cursor.close()
        Rslt = pd.DataFrame(Rslt,columns=['ID','指数名称','指数类型','交易所'])
        return Rslt
    # 给定指数名称和ID，获取指定日当前或历史上的指数中的股票ID，is_current=True:获取指定日当天的ID，False:获取截止指定日历史上出现的ID
    def getIndexStockID(self,index_name,index_id,date=getCurrentDateStr(),is_current=True,output_mode = 'list'):
        # 获取指数的Wind内部证券ID
        SQLStr = 'SELECT s_info_windcode FROM '+self.TablePrefix+'AIndexDescription where s_info_name=\''+index_name+'\' AND s_info_code=\''+index_id+'\''
        Cursor = self.execSQLQuery(SQLStr)
        temp = Cursor.fetchall()
        IndexEquityID = temp[0][0]
        Cursor.close()
        # 获取指数中的股票ID
        SQLStr = 'SELECT s_con_windcode FROM '+self.TablePrefix+'AIndexMembers WHERE s_info_windcode=\''+IndexEquityID+'\''
        SQLStr = SQLStr+' AND s_con_indate<=\''+date+'\''# 纳入日期在date之前
        if is_current:
            SQLStr = SQLStr+' AND (cur_sign=1 OR s_con_outdate>\''+date+'\')'# 剔除日期在date之后
        Cursor = self.execSQLQuery(SQLStr)
        WindCodes = [rslt[0] for rslt in Cursor.fetchall()]
        Cursor.close()
        IDs = self.WindCode2ID(WindCodes)
        IDs = list(IDs.values())
        IDs.sort()
        return IDs
    def GetStockIndustryClass(self,ids=None,industryName='中信行业',level=1,date_seq=None):
        '''获得个股在某个时间序列上所属行业名称'''
        if self.LibPath is None:
            raise("Error:WindDB.LibPath不能为空！")
        elif not os.path.isfile(self.LibPath+os.sep+'industriesClass.dat'):
            raise("Error:WindDB.LibPath文件夹中缺少industriesClass数据！")
        else:
            industryData = shelve.open(self.LibPath+os.sep+'industriesClass')
            industryData = industryData[industryName+str(level)]
        if ids is None:
            ids = self.GetAllAStock(is_current=False)
        windIndCode = self.getIndustryWindID(industryName,1)
        if isinstance(date_seq[0],int):
            date_seq = [str(x) for x in date_seq]
        ashareinsdustrydata = shelve.open(self.LibPath+os.sep+"ashareindustrydata")
        rslt = pd.DataFrame()
        for iID in ids:
            print(ids.index(iID))
            iStockIndInfo = industryData[industryData.windcode==iID]
            if len(iStockIndInfo) ==1 and iStockIndInfo.remove_dt.iloc[0] is None:
                stockIndustryName=windIndCode[iStockIndInfo.ind_code.iloc[0]]
                temp_data = pd.Series([stockIndustryName],index=iStockIndInfo.entry_dt.values,name=iID)
                temp_data = temp_data.reindex(date_seq,method='pad')
            else:
                stockIndustryName=[windIndCode[x] for x in iStockIndInfo.ind_code]
                temp_data = pd.Series(stockIndustryName,index=iStockIndInfo.entry_dt.values,name=iID)
                temp_data=temp_data.sort_index()
                temp_data = temp_data.reindex(date_seq,method='pad')
            rslt = pd.concat([rslt,temp_data],axis=1)
        ashareinsdustrydata[industryName+str(level)]=rslt
        ashareinsdustrydata.close()
        return 1

#朝阳永续数据库
class ZYYX(WindDB):
    def __init__(self, user="zyyx", password="zyyx", host="172.20.65.27", port="1521", dbname="cibfund",
                 data_lock=Lock(), wind_dict_path=None,
                 db_type='Oracle', lib_path=None, table_prefix='wind.'):
        self.User = user
        self.Password = password
        self.Host = host
        self.Port = port
        self.DBName = dbname
        self.Connection = None
        self.WindDictPath = wind_dict_path
        self.LibPath = lib_path
        self.Data = data_lock
        self.DBType = db_type
        self.TablePrefix = table_prefix
        if self.WindDictPath is not None:
            self.DataLock.acquire()
            DictFile = shelve.open(self.WindDictPath)
            self.TableInfo = DictFile['TableInfo']
            self.FactorInfo = DictFile['FactorInfo']
            DictFile.close()
            self.DataLock.release()
        return

class ShelveDB(object):
    """基于Shelve文件的自建数据库"""
    # 每个Shelve文件存储结构：{"__DataType":数据类型,因子名:DataFrame(index=日期,columns=ID)}
    def __init__(self,data_lock=Lock(),dir_path=ShelveDBPath):
        self.DataLock = data_lock
        self.DirPath = dir_path
        self.TableFactorDict = {}#{表名：[因子名]}
        self.updateInfo()
        return
    # 更新信息
    def updateInfo(self):
        AllFiles = os.listdir(self.DirPath)
        self.TableFactorDict = {}#{表名：[因子名]}
        self.DataLock.acquire()
        for iFile in AllFiles:
            iFileName = iFile.split('.')
            if (len(iFileName)>1) and (iFileName[-1]=='dat'):
                iFileName = '.'.join(iFileName[:-1])
                iDataFile = shelve.open(self.DirPath+os.sep+iFileName)
                self.TableFactorDict[iFileName] = list(iDataFile.keys())
                self.TableFactorDict[iFileName].remove("__DataType")
                iDataFile.close()
        self.DataLock.release()
        return 1
    # -------------------------------表的管理-----------------------------------------------------------------------------------
    # 获取Shelve数据库中的表名,ignore:忽略的表名列表
    def getTableName(self,ignore=[]):
        return [iTable for iTable in self.TableFactorDict if iTable not in ignore]
    # 获取一个可用的表名
    def genAvailableTableName(self):
        AllTableNames = self.getTableName()
        return genAvailableName('NewTable', AllTableNames)
    # 检查表是否存在
    def checkTableExistence(self,table_name):
        if table_name in self.TableFactorDict:
            return True
        else:
            return False
    # 创建表,field_name_types:字典{字段名:数据类型}，key=字段名，value=字段的数据类型,if_exists='cancel'：取消，'replace'：删除后重建,'error'：报错
    def createTable(self,table_name,field_name_types,if_exists='cancel'):
        isExist = self.checkTableExistence(table_name)
        if isExist:
            if if_exists=='cancel':
                return 0
            elif if_exists=='replace':
                self.deleteTable(table_name)
            else:
                raise Exception('该表已经存在')
        self.DataLock.acquire()
        NewFile = shelve.open(self.DirPath+os.sep+table_name)
        self.TableFactorDict[table_name] = list(field_name_types.keys())
        for iField in field_name_types:
            NewFile[iField] = pd.DataFrame()
        NewFile['__DataType'] = pd.Series(field_name_types)
        NewFile.close()
        self.DataLock.release()
        return 1
    # 添加表,file_path:表的文件名，dir_path:表文件夹，将该文件夹下的表都添加进来,返回{旧表名:复制后的新表名}
    def addTable(self,file_path=None,dir_path=None):
        TableNameDict = {}
        if file_path is not None:
            OldTableName = file_path.split(os.sep)[-1]
            if OldTableName in self.TableFactorDict:
                NewTableName = genAvailableName(OldTableName,self.TableFactorDict)
            else:
                NewTableName = OldTableName
            #数据拷贝
            if os.path.isfile(file_path+'.dat'):
                shutil.copy(file_path+'.dat',self.DirPath+os.sep+NewTableName+'.dat')
            if os.path.isfile(file_path+'.bak'):
                shutil.copy(file_path+'.bak',self.DirPath+os.sep+NewTableName+'.bak')
            if os.path.isfile(file_path+'.dir'):
                shutil.copy(file_path+'.dir',self.DirPath+os.sep+NewTableName+'.dir')
            self.DataLock.acquire()
            DataFile = shelve.open(self.DirPath+os.sep+NewTableName)
            self.TableFactorDict[NewTableName] = list(DataFile.keys())
            DataFile.close()
            self.DataLock.release()
            self.TableFactorDict[NewTableName].remove('__DataType')
            TableNameDict = {file_path:self.DirPath+os.sep+NewTableName}
        if dir_path is not None:
            AllFiles = os.listdir(dir_path)
            for iFile in AllFiles:
                iFileName = iFile.split('.')
                if (len(iFileName)>1) and (iFileName[-1]=='dat'):
                    TableNameDict.update(self.addTable(file_path=dir_path+os.sep+'.'.join(iFileName[:-1])))
        return TableNameDict
    def renameTable(self,old_table_name,new_table_name):
        if not self.checkTableExistence(old_table_name):
            raise Exception('该表不存在')
        self.TableFactorDict[new_table_name] = self.TableFactorDict.pop(old_table_name)
        FileName = self.DirPath+os.sep+old_table_name+'.dat'
        if os.path.isfile(FileName):
            os.rename(FileName,self.DirPath+os.sep+new_table_name+'.dat')
        FileName = self.DirPath+os.sep+old_table_name+'.bak'
        if os.path.isfile(FileName):
            os.rename(FileName,self.DirPath+os.sep+new_table_name+'.bak')
        FileName = self.DirPath+os.sep+old_table_name+'.dir'
        if os.path.isfile(FileName):
            os.rename(FileName,self.DirPath+os.sep+new_table_name+'.dir')
        return 1
    # 删除表
    def deleteTable(self,table_name):
        if not self.checkTableExistence(table_name):
            return 0
        self.TableFactorDict.pop(table_name)
        FileName = self.DirPath+os.sep+table_name+'.dat'
        if os.path.isfile(FileName):
            os.remove(FileName)
        FileName = self.DirPath+os.sep+table_name+'.bak'
        if os.path.isfile(FileName):
            os.remove(FileName)
        FileName = self.DirPath+os.sep+table_name+'.dir'
        if os.path.isfile(FileName):
            os.remove(FileName)
        return 1     
    # 获取这张表中公共的日期序列
    def getTableDate(self,table_name,start_date=None,end_date=None):
        self.DataLock.acquire()
        DataFile = shelve.open(self.DirPath+os.sep+table_name)
        Dates = None
        for iFactor in DataFile:
            if iFactor!='__DataType':
                if Dates is None:
                    Dates = set(DataFile[iFactor].index)
                else:
                    Dates = Dates.intersection(set(DataFile[iFactor].index))
        DataFile.close()
        self.DataLock.release()
        Dates = list(Dates)
        Dates.sort()
        if start_date is not None:
            Dates = [iDate for iDate in Dates if iDate>=start_date]
        if end_date is not None:
            Dates = [iDate for iDate in Dates if iDate<=end_date]
        return Dates
    # 获取表的公共ID
    def getTableID(self,table_name):
        self.DataLock.acquire()
        DataFile = shelve.open(self.DirPath+os.sep+table_name)
        IDs = None
        for iFactor in DataFile:
            if iFactor != '__DataType':
                if IDs is None:
                    IDs = set(DataFile[iFactor].columns)
                else:
                    IDs = IDs.intersection(set(DataFile[iFactor].columns))
        DataFile.close()
        self.DataLock.release()
        IDs = list(IDs)
        IDs.sort()
        return IDs
    # 获取表的完整路径
    def getTablePath(self,table_name):
        if not os.path.isfile(table_name+'.dat'):
            return self.DirPath+os.sep+table_name
        else:
            return table_name
    # 合并表，时间序列相连，若new_table_name非None，将合并保存至new_table_name,否则覆盖table_name1,if_overlapping表示index有重复时的处理方式,可取'update'：使用2的值，'retain'：使用1的值
    def appendTable(self,table_name1,table_name2,new_table_name=None,if_overlapping='update'):
        TempTableName = genAvailableName('TempTable',list(self.TableFactorDict.keys())+[new_table_name])
        DataType1 = self.getDataType(table_name1)
        DataType2 = self.getDataType(table_name2)
        self.DataLock.acquire()
        DataFile = shelve.open(self.getTablePath(TempTableName))
        DataFile1 = shelve.open(self.getTablePath(table_name1))
        DataFile2 = shelve.open(self.getTablePath(table_name2))
        AllFactorNames = list(set(self.TableFactorDict[table_name1]).union(set(self.TableFactorDict[table_name2])))
        DataType = {}
        for iFactorName in AllFactorNames:
            iFactorData1 = DataFile1.get(iFactorName)
            iFactorData2 = DataFile2.get(iFactorName)
            if (iFactorData1 is not None) and (iFactorData2 is not None):
                if if_overlapping=='update':
                    NewIndex = list(set(iFactorData1.index).difference(set(iFactorData2.index)))
                    iFactorData1 = iFactorData2.append(iFactorData1.ix[NewIndex])
                elif if_overlapping=='retain':
                    NewIndex = list(set(iFactorData2.index).difference(set(iFactorData1.index)))
                    iFactorData1 = iFactorData1.append(iFactorData2.ix[NewIndex])
                DataType[iFactorName] = DataType1[iFactorName]
            elif iFactorData1 is None:
                iFactorData1 = iFactorData2
                DataType[iFactorName] = DataType2[iFactorName]
            else:
                DataType[iFactorName] = DataType1[iFactorName]
            iFactorData1 = iFactorData1.sort_index()
            DataFile[iFactorName] = iFactorData1
        DataFile["__DataType"] = pd.Series(DataType)
        DataFile.close()
        DataFile1.close()
        DataFile2.close()
        self.DataLock.release()
        self.TableFactorDict[TempTableName] = AllFactorNames
        if new_table_name is None:
            self.deleteTable(table_name1)
            self.renameTable(TempTableName,table_name1)
        elif new_table_name in self.TableFactorDict:
            self.deleteTable(new_table_name)
            self.renameTable(TempTableName,new_table_name)
        else:
            self.renameTable(TempTableName,new_table_name)
        return 1
    #对数据表瘦身
    def shrinkTable(self,table_name):
        if not self.checkTableExistence(table_name):
            return 0
        new_table_name = genAvailableName(table_name,self.TableFactorDict)
        self.DataLock.acquire()
        OldDataFile = shelve.open(self.getTablePath(table_name))
        NewDataFile = shelve.open(self.DirPath+os.sep+new_table_name)
        NewDataFile['__DataType']=OldDataFile['__DataType']
        for iField in self.getFieldName(table_name,['__DataType']):
            NewDataFile[iField]=OldDataFile[iField]
        NewDataFile.close()
        OldDataFile.close()
        self.DataLock.release()
        self.TableFactorDict[new_table_name]=self.TableFactorDict[table_name]
        self.deleteTable(table_name)
        self.renameTable(new_table_name,table_name)
        return 1
    # --------------------------------字段管理----------------------------------------------------------------------------
    # 获取一张表的所有字段名
    def getFieldName(self,table_name,ignore=[]):
        ignore += ['__DataType']
        return [iField for iField in self.TableFactorDict[table_name] if iField not in ignore]
    # 获取某个因子的日期序列
    def getFactorDate(self,table_name,factor_name,start_date=None,end_date=None):
        self.DataLock.acquire()
        DataFile = shelve.open(self.DirPath+os.sep+table_name)
        Dates = DataFile[factor_name].index
        DataFile.close()
        self.DataLock.release()
        Dates = list(Dates)
        Dates.sort()
        if start_date is not None:
            Dates = [iDate for iDate in Dates if iDate>=start_date]
        if end_date is not None:
            Dates = [iDate for iDate in Dates if iDate<=end_date]
        return Dates
    # 获取某个因子的ID序列
    def getFactorID(self,table_name,factor_name):
        self.DataLock.acquire()
        DataFile = shelve.open(self.DirPath+os.sep+table_name)
        IDs = DataFile[factor_name].columns
        DataFile.close()
        self.DataLock.release()
        IDs = list(IDs)
        IDs.sort()
        return IDs      
    # 增加字段，field_name_types字典{字段名:数据类型},如果新字段与旧字段重复，忽略新字段
    def addField(self,table_name,field_name_types):
        new_fields=list(set(field_name_types.keys()).difference(set(self.TableFactorDict[table_name])))
        if not new_fields:
            return 0
        field_name_types={x:field_name_types[x] for x in new_fields}
        self.TableFactorDict[table_name] += new_fields
        self.DataLock.acquire()
        DataFile = shelve.open(self.getTablePath(table_name))
        for iField in new_fields:
            DataFile[iField] = pd.DataFrame()
        DataType = DataFile['__DataType']
        DataFile['__DataType'] = DataType.append(pd.Series(field_name_types))
        DataFile.close()
        self.DataLock.release()
        return 1
    # 对一张表的字段进行重命名
    def renameField(self,table_name,old_fields,new_fields):
        self.DataLock.acquire()
        DataFile = shelve.open(self.DirPath+os.sep+table_name)
        OldDataType = DataFile['__DataType']
        NewDataType = pd.Series(old_fields,index=new_fields)
        for i,iOldField in enumerate(old_fields):
            self.TableFactorDict[table_name].remove(iOldField)
            self.TableFactorDict[table_name].append(new_fields[i])
            DataFile[new_fields[i]] = DataFile.pop(iOldField)
            NewDataType.iloc[i] = OldDataType.ix[iOldField]
            OldDataType = OldDataType.drop(iOldField)
        NewDataType = NewDataType.append(OldDataType)
        DataFile['__DataType'] = NewDataType
        DataFile.close()
        self.DataLock.release()
        return 1
    # 删除一张表中的某些字段
    def deleteField(self,table_name,field_names):
        self.DataLock.acquire()
        DataFile = shelve.open(self.DirPath+os.sep+table_name)
        NewDataType = DataFile['__DataType']
        for iField in field_names:
            self.TableFactorDict[table_name].remove(iField)
            DataFile.pop(iField)
            NewDataType = NewDataType.drop(iField)
        DataFile['__DataType'] = NewDataType
        DataFile.close()
        self.DataLock.release()
        return 1
    # 合并因子，时间序列相连，若new_table_name非None，将合并保存至new_table_name,否则覆盖table_name1,if_overlapping表示index有重复时的处理方式,可取'update'：使用2的值，'retain'：使用1的值
    def appendFactor(self,table_name1,table_name2,factor_name1,factor_name2,new_table_name=None,new_factor_name=None,if_overlapping='update'):
        DataType = pd.Series(self.getDataType(table_name1))        
        if new_table_name is None:
            new_table_name = table_name1
        else:
            self.TableFactorDict[new_table_name] = [factor_name1]
            DataType = DataType.ix[[factor_name1]]
        FactorData1 = self.loadFactor(table_name1,factor_name1)
        FactorData2 = self.loadFactor(table_name2,factor_name2)
        if if_overlapping=='update':
            NewIndex = list(set(FactorData1.index).difference(set(FactorData2.index)))
            FactorData1 = FactorData2.append(FactorData1.ix[NewIndex])
        elif if_overlapping=='retain':
            NewIndex = list(set(FactorData2.index).difference(set(FactorData1.index)))
            FactorData1 = FactorData1.append(FactorData2.ix[NewIndex])
        FactorData1 = FactorData1.sort_index()
        self.DataLock.acquire()
        DataFile = shelve.open(self.getTablePath(new_table_name))
        DataFile[factor_name1] = FactorData1
        DataFile["__DataType"] = DataType
        DataFile.close()
        self.DataLock.release()
        return 1
    # 产生因子的数据类型，'string','double'
    def genDataType(self,dtypes):
        if np.dtype('O') in dtypes.values:
            return 'string'
        else:
            return 'double'
    # 获取因子的数据类型，'string','double',返回{因子名:数据类型}
    def getDataType(self,table_name,field_names=None):
        if not os.path.isfile(table_name+'.dat'):
            table_name = self.DirPath+os.sep+table_name
        self.DataLock.acquire()
        DataFile = shelve.open(table_name)
        if field_names is None:
            FieldDataTypeDict = dict(DataFile['__DataType'])
        else:
            FieldDataTypeDict = dict(DataFile['__DataType'].ix[field_names])#{因子名:数据类型}
        DataFile.close()
        self.DataLock.release()
        return FieldDataTypeDict
    # 获取某张表中某个字段的所有值（不同）
    def getFactorUniqueValue(self,table_name,field_name,is_sorted=True,ignore=[None]):
        self.DataLock.acquire()
        DataFile = shelve.open(self.getTablePath(table_name))
        DataFile[field_name]=DataFile[field_name].where(pd.notnull(DataFile[field_name]),None)
        Rslt = np.unique(DataFile[field_name].values[DataFile[field_name].values!=np.array(None)])
        DataFile.close()
        self.DataLock.release()
        if is_sorted:
            Rslt.sort()
        return [iRslt for iRslt in Rslt if iRslt not in ignore]
    # 移动因子,并不删除原来的因子,table_factor:{表名:[因子名]},如果有重名，将把原来的因子名添加数字后缀作为新因子名
    def moveFactor(self,table_factor,target_table):
        if table_factor == {}:
            return (0,"没有因子选中!")
        TargetFile = shelve.open(filename=self.getTablePath(target_table))
        AllFactorNames = list(TargetFile.keys())
        DataType = {}
        for iTable in table_factor:
            iDataFile = shelve.open(filename=self.getTablePath(iTable))
            iDataType = iDataFile["__DataType"]
            if table_factor[iTable] is None:
                iFactorNames = self.TableFactorDict[iTable]
            else:
                iFactorNames = table_factor[iTable]
            for ijFactor in iFactorNames:
                if ijFactor in AllFactorNames:
                    ijNewFactor = genAvailableName(header=ijFactor,all_names=AllFactorNames)
                    TargetFile[ijNewFactor] = iDataFile[ijFactor]
                    DataType[ijNewFactor] = iDataType[ijFactor]
                    self.TableFactorDict[TargetTableName].append(ijNewFactor)
                    AllFactorNames.append(ijNewFactor)
                else:
                    TargetFile[ijFactor] = iDataFile[ijFactor]
                    DataType[ijFactor] = iDataType[ijFactor]
                    self.TableFactorDict[TargetTableName].append(ijFactor)
                    AllFactorNames.append(ijFactor)
            iDataFile.close()
        TargetFile["__DataType"] = pd.Series(DataType).append(TargetFile["__DataType"])
        TargetFile.close()
        return (1,"因子移动完成!")
    # ---------------------------数据管理-------------------------------
    # 日期平移，沿着日期轴将所有数据纵向移动lag期,lag>0向后移动,lag<0向前移动，空出来的地方填nan,table_factor:{表名 : [因子名]}
    def translateDate(self,table_factor,lag=1,target_table=None):
        self.DataLock.acquire()
        if target_table is not None:
            TempTable = genAvailableName('TempTable',list(self.TableFactorDict.keys())+[target_table])
            TargetFile = shelve.open(self.getTablePath(TempTable))
            DataType = {}
        for iTable in table_factor:
            iDataFile = shelve.open(self.getTablePath(iTable))
            iDataType = iDataFile['__DataType']
            if table_factor[iTable] is None:
                iFactorNames = list(iDataType.index)
            else:
                iFactorNames = table_factor[iTable]
            for jFactor in iFactorNames:
                ijDF = iDataFile[jFactor]
                ijTemp = pd.DataFrame(index=ijDF.index,columns=ijDF.columns)
                if lag>0:
                    ijTemp.iloc[lag:,:] = ijDF.iloc[:-lag,:].values
                elif lag<0:
                    ijTemp.iloc[:lag,:] = ijDF.iloc[-lag:,:].values
                else:
                    ijTemp = ijDF
                if target_table is not None:
                    TargetFile[jFactor] = ijTemp
                    DataType[jFactor] = iDataType[jFactor]
                else:
                    iDataFile[jFactor] = ijTemp
            iDataFile.close()
        if target_table is not None:
            TargetFile['__DataType'] = pd.Series(DataType)
            TargetFile.close()
            self.TableFactorDict[TempTable] = list(DataType.keys())
            if target_table in self.TableFactorDict:
                self.deleteTable(target_table)
            self.renameTable(TempTable,target_table)
        self.DataLock.release()
        return 1
    # 日期变换，对原来的日期序列通过某种变换函数得到新的日期序列，调整数据.table_factor:{表名:[因子名]},如果table_name为None，则覆盖原表，否则创建新表
    def changeDate(self,table_factor,table_name=None,date_change_fun=None,dates=None):
        self.DataLock.acquire()
        if table_name is None:# 覆盖原文件
            for iTable in table_factor:
                if table_factor[iTable] is None:
                    FactorNames = self.TableFactorDict[iTable]
                else:
                    FactorNames = table_factor[iTable]
                iDataFile = shelve.open(self.getTablePath(iTable))
                for ijFactor in FactorNames:
                    ijFactorData = iDataFile[ijFactor]
                    if dates is not None:
                        ijFactorData = ijFactorData.loc[dates]
                    if date_change_fun is not None:
                        ijDates = list(ijFactorData.index)
                        ijDates = date_change_fun(ijDates)
                        ijFactorData = ijFactorData.loc[ijDates]
                    iDataFile[ijFactor] = ijFactorData
                iDataFile.close()
        else:
            DataType = {}
            TempTable = genAvailableName('TempTable',list(self.TableFactorDict.keys())+[table_name])
            DataFile = shelve.open(self.getTablePath(TempTable))
            for iTable in table_factor:
                if table_factor[iTable] is None:
                    FactorNames = self.TableFactorDict[iTable]
                else:
                    FactorNames = table_factor[iTable]
                iDataFile = shelve.open(self.getTablePath(iTable))
                iDataType = iDataFile["__DataType"]
                for ijFactor in FactorNames:
                    ijFactorData = iDataFile[ijFactor]
                    if dates is not None:
                        ijFactorData = ijFactorData.loc[dates]
                    if date_change_fun is not None:
                        ijDates = list(ijFactorData.index)
                        ijDates = date_change_fun(ijDates)
                        ijFactorData = ijFactorData.loc[ijDates]
                    DataFile[ijFactor] = ijFactorData
                    DataType[ijFactor] = iDataType[ijFactor]
                iDataFile.close()
            DataFile["__DataType"] = pd.Series(DataType)
            DataFile.close()
            self.TableFactorDict[TempTable] = list(DataType.keys())
            if table_name in self.TableFactorDict:
                self.deleteTable(table_name)
            self.renameTable(TempTable,table_name)
        self.DataLock.release()
        return (1,"日期变换成功!")

    # ----------------------数据写入-------------------------------------
    # 将因子数据存入Shelve数据库;if_exists:append,replace;factor_data格式:DataFrame(index=日期,columns=ID)
    def saveFactor(self,factor_data,table_name,factor_name,if_exists='append',data_type=None):
        self.DataLock.acquire()
        DataFile = shelve.open(self.getTablePath(table_name))
        if table_name not in self.TableFactorDict:
            self.TableFactorDict[table_name] = []
        if "__DataType" not in DataFile:
            DataFile['__DataType'] = pd.Series()
        isExist = (factor_name in DataFile)
        if not isExist:
            DataFile[factor_name] = factor_data
            self.TableFactorDict[table_name].append(factor_name)
            NewDataType = DataFile['__DataType']
            if data_type is None:
                NewDataType = NewDataType.append(pd.Series({factor_name:self.genDataType(factor_data.dtypes)}))
            else:
                NewDataType = NewDataType.append(pd.Series({factor_name:data_type}))
            DataFile['__DataType'] = NewDataType
        elif if_exists=='append':
            OldDates = set(DataFile[factor_name].index)
            NewDates = list(set(factor_data.index).difference(OldDates))
            NewData = DataFile[factor_name].append(factor_data.ix[NewDates,:])
            DataFile[factor_name] = NewData.sort_index()
        elif if_exists=='replace':  
            DataFile[factor_name] = factor_data
            NewDataType = DataFile['__DataType']
            if factor_name in NewDataType.index:
                NewDataType = NewDataType.drop(factor_name)
            if data_type is None:
                NewDataType = NewDataType.append(pd.Series({factor_name:self.genDataType(factor_data.dtypes)}))
            else:
                NewDataType = NewDataType.append(pd.Series({factor_name:data_type}))
            DataFile['__DataType'] = NewDataType
        DataFile.close()
        self.DataLock.release()
        return 1
    # 将CSV中的数据写入某张表中的某个因子,每一列对应一个ID,if_date_inconformity:为'csv'表示以csv中的日期为准，'table':表示以表中的日期为准
    # 为'intersection'表示取交集，'union'表示取并集.注：csv_path中不可有中文字符，if_id_inconformity：为'csv'表示当ID有重叠时以csv中的ID为准，'table'：表示以表中的ID为准
    def importCSV2Factor(self,csv_path,table_name,factor_name,if_date_inconformity='union',if_id_inconformity='csv'):
        CSVFactor = pd.read_csv(csv_path,index_col=0,header=0)
        CSVDate = [str(iDate) for iDate in CSVFactor.index]
        CSVFactor = pd.DataFrame(CSVFactor.values,index=CSVDate,columns=CSVFactor.columns)
        if table_name not in self.TableFactorDict:
            self.TableFactorDict[table_name] = [factor_name]
            FinalFactor = CSVFactor
            DataType = pd.Series({factor_name:self.genDataType(CSVFactor.dtypes)})
        elif factor_name not in self.TableFactorDict[table_name]:
            self.TableFactorDict[table_name].append(factor_name)
            FinalFactor = CSVFactor
            DataType = self.getDataType(table_name)
            DataType = pd.Series(DataType).append(pd.Series({factor_name:self.genDataType(CSVFactor.dtypes)}))
        else:
            OldFactor = self.loadFactor(table_name,factor_name)
            CSVID = CSVFactor.columns
            OldID = OldFactor.columns
            if if_id_inconformity=='csv':
                OldFactor = OldFactor.loc[:,list(set(OldID).difference(set(CSVID)))]
            else:
                CSVFactor = CSVFactor.loc[:,list(set(CSVID).difference(set(OldID)))]
            if if_date_inconformity=='union':
                FinalFactor = pd.merge(CSVFactor,OldFactor,left_index=True,right_index=True,how='outer')
            elif if_date_inconformity=='intersection':
                FinalFactor = pd.merge(CSVFactor,OldFactor,left_index=True,right_index=True,how='inner')
            elif if_date_inconformity=='csv':
                FinalFactor = pd.merge(CSVFactor,OldFactor,left_index=True,right_index=True,how='left')
            else:
                FinalFactor = pd.merge(CSVFactor,OldFactor,left_index=True,right_index=True,how='right')
            DataType = pd.Series(self.getDataType(table_name))
        self.DataLock.acquire()
        with closing(shelve.open(self.DirPath+os.sep+table_name)) as File:
            File[factor_name] = FinalFactor
            File['__DataType'] = DataType
        self.DataLock.release()
        return 1
    #将mat文件的因子导入到数据库
    def importMat2Factor(self,mat_file_path,factorID,table_name,factor_name,if_exists='append',start_date=None,end_date=None,data_type=None):
        motherPath=os.sep.join(re.split(r'[/\\]',mat_file_path)[:-1])
        dirName=re.split(r'[/\\]',mat_file_path)[-1]
        matDB=MatDB(dir_path=motherPath)
        data=matDB.loadFactor(dirName,'output',factorID,start_date=start_date,end_date=end_date)
        self.DataLock.acquire()
        DataFile=shelve.open(self.getTablePath(table_name))
        if table_name not in self.TableFactorDict:
            self.TableFactorDict[table_name]=[]
        if "__DataType" not in DataFile:
            DataFile['__DataType']=pd.Series()
        isExist=(factor_name in DataFile)
        if not isExist:
            DataFile[factor_name]=data
            self.TableFactorDict[table_name].append(factor_name)
            NewDataType=DataFile['__DataType']
            if data_type is None:
                NewDataType=NewDataType.append(pd.Series({factor_name:self.genDataType(data.dtypes)}))
            else:
                NewDataType=NewDataType.append(pd.Series({factor_name:data_type}))
            DataFile['__DataType']=NewDataType
        elif if_exists=='append':
            OldDates=set(DataFile[factor_name].index)
            NewDates=list(set(data.index).difference(OldDates))
            NewData=DataFile[factor_name].append(data.ix[NewDates,:])
            DataFile[factor_name]=NewData.sort_index()
        elif if_exists=='replace':
            DataFile[factor_name]=data
            NewDataType=DataFile['__DataType']
            if factor_name in NewDataType.index:
                NewDataType=NewDataType.drop(factor_name)
            if data_type is None:
                NewDataType=NewDataType.append(pd.Series({factor_name:self.genDataType(data.dtypes)}))
            else:
                NewDataType=NewDataType.append(pd.Series({factor_name:data_type}))
            DataFile['__DataType']=NewDataType
            DataFile.close()
        self.DataLock.release()
        return 1
    # ------------------------数据读取--------------------------------------
    # 加载Shelve数据库中的因子到内存中
    def loadFactor(self,table_name,factor_name,start_date=None,end_date=None,dates=None,ids=None,**kwargs):
        self.DataLock.acquire()
        DataFile = shelve.open(self.getTablePath(table_name))
        if ids is None:
            Rslt = DataFile[factor_name]
        else:
            if not isinstance(ids,list):
                ids = [ids]
            Rslt = DataFile[factor_name].ix[:,ids]
        DataFile.close()
        self.DataLock.release()
        if start_date is not None:
            Rslt = Rslt.ix[start_date:]
        if end_date is not None:
            Rslt = Rslt.ix[:end_date]
        if dates is not None:
            if not isinstance(dates,list):
                dates = [dates]
            Rslt = Rslt.ix[dates]
        Rslt = Rslt.copy()
        gc.collect()
        return Rslt
    #加载横截面数据
    def loadCrossSectionData(self,tableFactors,date,ids=None):
        Rslt=pd.DataFrame()
        factors=[]
        self.DataLock.acquire()
        for iTable in tableFactors:
            for ijFactor in tableFactors[iTable]:
                factors.append(ijFactor)
                ijData = self.loadFactor(iTable,ijFactor,dates=[date],ids=ids)
                Rslt=pd.concat([Rslt,ijData.T],axis=1,ignore_index=True)
        Rslt.columns=factors
        self.DataLock.release()
        return Rslt
    # 将Shelve数据库中的因子写到csv文件
    def writeFactor2CSV(self,csv_path,table_name,factor_name,start_date=None,end_date=None,dates=None,ids=None):
        Rslt = self.loadFactor(table_name, factor_name, start_date,end_date,dates,ids)
        Rslt.to_csv(csv_path)
        return 1
    def writeTable2SQLite(self,table_name,sqlite_name):
        ignore.append('__DataType')
        conn=sqlite3.connect(sqliteDB+os.sep+sqlite_name)
        data=pd.DataFrame()
        keys=[]
        tablePath=self.getTablePath(table_name)
        with closing(shelve.open(tablePath)) as f:
            for iKey in f.keys():
                if iKey in ignore:
                    continue
                print("转换%s因子"%iKey)
                keys.append(iKey)
                tempData=f[iKey]
                tempData=tempData.stack()
                data=pd.concat([data,tempData],axis=1)
        data.columns=keys
        tempData.to_sql(iKey,conn,index=True,if_exists='replace')
        conn.close()
        print("%s表转换完成"%shelve_table)
        return 1
    # 将Shelve数据库中的因子写到HDF5文件,HDF5文件格式为【日期 股票ID 因子值】
    def writeFactor2H5(self,h5_path,table_name,factor_name,start_date=None,end_date=None,dates=None,ids=None):
        Rslt=self.loadFactor(table_name,factor_name,start_date,end_date,dates,ids)
        # 对因子表格降维处理
        Rslt=Rslt.stack(dropna=False)
        index=Rslt.index.levels
        dates_and_ids=np.array(list(itertools.product(index[0],index[1])))
        newTable=np.hstack((dates_and_ids,Rslt.values.reshape(-1,1)))
        newTable=pd.DataFrame(newTable,columns=['Dates','IDs','Values'])
        #从目标路径中提取文件名        
        h5FileName=re.split(r'[\\/]',h5_path)[-1]
        h5FileName=h5FileName.strip('.h5')
        try:
            if os.path.isfile(h5_path):
                h5File=pd.HDFStore(h5_path)
                oldData=h5File[h5FileName]
                oldDates=oldData['Dates'].values.tolist()
                newDates=list(set(newTable['Dates'].values).difference(oldDates))
                temp=newTable['Dates'].isin(newDates)
                h5File.append(h5FileName,newTable[temp],append=True,data_columns=True,index=False)
            else:
                h5File=pd.HDFStore(h5_path)
                h5File.append(h5FileName,newTable,append=True,data_columns=True,index=False)
            h5File.create_table_index(h5FileName,columns=['Dates'],optlevel=9,kind='full')
        except:
            print('HDF5数据导出失败！')
            h5File.close()
            return 0
        h5File.close()
        return 1
    # 将数据库中的因子写入到mat文件,mat文件格式为【日期 股票ID 因子ID 因子值】
    def writeFactor2Mat(self,mat_file_path,table_name,factor_name,start_date=None,end_date=None,dates=None,ids=None):
        #检验目标路径是否存在
        if not os.path.isdir(mat_file_path):
            raise('目标路径不存在，请更改路径！')
        Rslt=self.loadFactor(table_name,factor_name,start_date,end_date,dates,ids)
        factorID=int(((np.random.rand(1)+1)*1000)[0])
        for idate in Rslt.index.tolist():
            iData=Rslt.ix[idate]
            nRowsOfData=len(iData)
            newTable=np.zeros((nRowsOfData,4))
            newTable[:,0]=np.array([idate]*nRowsOfData).astype('int64')
            windcodes=[x+'.SH' if x[0]=='6' else x+'.SZ' for x in iData.index]
            newTable[:,1]=np.array(CodeConvert(windcodes)).astype('int64')
            newTable[:,2]=np.array([factorID]*nRowsOfData).astype('int64')
            newTable[:,3]=iData.values.reshape((-1,))
            sio.savemat(mat_file_path+os.sep+'%s.mat'%idate,{'output':newTable})
        return 1
#----------------------------------------------HDF5数据库-----------------------------------------------------------------------------
"""每个文件是一个因子，存储结构：【日期 ID 因子值】"""
class HDF5DB(object):
    def __init__(self,data_lock=Lock(),dir_path='.'):
        self.DataLock=data_lock
        self.DirPath=dir_path
        self.FactorDict=[] #因子名
        self.updateInfo()
        return
    #更新信息
    def updateInfo(self):
        AllFiles=os.listdir(self.DirPath)
        self.FactorDict=[]
        self.DataLock.acquire()
        for iFile in AllFiles:
            iFileName=iFile.split('.')
            if (len(iFileName)>1) and (iFileName[-1]=='h5'):
                self.FactorDict.append('.'.join(iFileName[:-1]))
        self.DataLock.release()
        return 1
    #---------------------------------------------表的管理-----------------------------------------------------------------------------------
    #获取数据库的表名
    def getTableName(self,ignore=[]):
        return [iTable for iTable in self.FactorDict if iTable not in ignore]
    #检查表是否存在
    def checkTableExistence(self,table_name):
        if table_name in self.FactorDict:
            return True
        else:
            return False
    #删除表
    def deleteTable(self,table_name):
        if not self.checkTableExistence(table_name):
            return 0
        self.FactorDict.pop(self.FactorDict.index(table_name))
        FileName=self.DirPath+os.sep+table_name+'.h5'
        if os.path.isfile(FileName):
            os.remove(FileName)
        return 1
    #获取这张表的公共日期
    def getTableDate(self,table_name,start_date=None,end_date=None):
        self.DataLock.acquire()
        DataFile=pd.HDFStore(self.DirPath+os.sep+table_name+'.h5')
        Dates=DataFile.select_column(table_name,'Dates')
        Dates=Dates.drop_duplicates().sort_values()
        DataFile.close()
        self.DataLock.release()
        Dates=Dates.values.tolist()
        if start_date is not None:
            Dates=[iDate for iDate in Dates if iDate>=start_date]
        if end_date is not None:
            Dates=[iDate for iDate in Dates if iDate<=end_date]
        return Dates
    #获取表的公共ID
    def getTableID(self,table_name):
        self.DataLock.acquire()
        DataFile=pd.HDFStore(self.DirPath+os.sep+table_name+'.h5')
        IDs=DataFile.select_column(table_name,'IDs')
        IDs.drop_duplicates(inplace=True)
        DataFile.close()
        self.DataLock.release()
        IDs=IDs.values.tolist()
        IDs.sort()
        return IDs
    #获取表的完整路径
    def getTablePath(self,table_name):
        if not os.path.isfile(table_name+'.h5'):
            return self.DirPath+os.sep+table_name
        else:
            return table_name
    #-------------------------------------------- 字段管理-------------------------------------------------------------------------
    #获取一张表的字段名
    def getFieldName(self,table_name):
        self.DataLock.acquire()
        DataFile=pd.HDFStore(self.DirPath+os.sep+table_name+'.h5')
        FieldName=list(DataFile.keys())
        DataFile.close()
        self.DataLock.release()
        return FieldName
    #--------------------------------------------数据管理---------------------------------------------------------------------------
    #加载因子到内存
    def loadFactor(self,table_name,factor_name,start_date=None,end_date=None,dates=None,ids=None):
        self.DataLock.acquire()
        DataFile=pd.HDFStore(self.getTablePath(table_name)+'.h5')
        if (dates is None) and (start_date is None) and (end_date) is None:
            Rslt=DataFile[table_name]
        elif dates is not None:
            start_date=min(dates)
            end_date=max(dates)
            Rslt=DataFile.select(table_name,where="Dates>=start_date and Dates<=end_date")
        else:
            Rslt=DataFile.select(table_name,where="Dates>=start_date and Dates<=end_date")
        DataFile.close()
        self.DataLock.release()
        if ids is not None:
            Rslt=Rslt[Rslt['IDs'].isin(ids)]
        DatesArr=Rslt['Dates'].drop_duplicates().sort_values().values
        IDsArr=Rslt['IDs'].drop_duplicates().sort_values().values
        nrow=len(DatesArr)
        ncol=len(IDsArr)
        newTable=np.array([np.nan]*nrow*ncol).reshape((nrow,ncol))
        for irow,iDate in enumerate(DatesArr):
            iData=Rslt[Rslt['Dates']==iDate].sort_values('IDs')
            temp=np.in1d(IDsArr,iData['IDs'].values)
            newTable[irow,temp]=iData['Values'].values
        newTable=pd.DataFrame(newTable,columns=IDsArr,index=DatesArr)
        gc.collect()
        return newTable
    
#----------------------------------------------Mat文件数据库----------------------------------------------------------------------------
#以Mat文件构成的数据库,数据结构：每个子文件夹代表一个因子，子文件夹里每一天一个mat文件代表每一天的因子值
class MatDB(object):
    def __init__(self,data_lock=Lock(),dir_path='.'):
        self.DataLock=data_lock
        self.DirPath=dir_path
        self.FactorDict=[]
        self.updateInfo()
        return
    def updateInfo(self):
        self.FactorDict=os.listdir(self.DirPath)
        return 1
    #-----------------------------------------表的管理----------------------------------------------------------------------------------
    #检查表是否存在
    def checkTableExistence(self,table_name):
        if table_name in self.FactorDict:
            return True
        else:
            return False
    #获取数据库表名
    def getTableName(self,ignore=[]):
        return [iTable for iTable in self.FactorDict if iTable not in ignore]
    #删除表
    def deleteTable(self,table_name):
        if not self.checkTableExistence(table_name):
            return 0
        self.FactorDict.pop(self.FactorDict.index(table_name))
        tableName=self.DirPath+os.sep+table_name
        if os.path.isdir(tableName):
            shutil.rmtree(tableName)
        return 1
    #获取这张表的公共日期
    def getTableDate(self,table_name,start_date=None,end_date=None):
        tableName=self.DirPath+os.sep+table_name
        AllFiles=os.listdir(tableName)
        dates=[iDate[:-4] for iDate in AllFiles if iDate[-4:]=='.mat']
        dates.sort()
        if start_date is not None:
            dates=[iDate for iDate in dates if iDate>=start_date]
        if end_date is not None:
            dates=[iDate for iDate in dates if iDate<=end_date]
        return dates
    #获取表的完整路径
    def getTablePath(self,table_name):
        if not os.path.isdir(table_name):
            return self.DirPath+os.sep+table_name
        else:
            return table_name
    #--------------------------------------数据管理-----------------------------------------------------------------------------------
    #加载因子到内存！
    def loadFactor(self,table_name,factor_name,factor_id,start_date=None,end_date=None,dates=None,ids=None):
        allDates=self.getTableDate(table_name)
        self.DataLock.acquire()
        Rslt=pd.DataFrame()
        tablePath=self.getTablePath(table_name)
        if (dates is None) and (start_date is None) and (end_date) is None:
            for idate in allDates:
                print('正在读取【%s】数据'%idate)
                iData=sio.loadmat(tablePath+os.sep+idate+'.mat',squeeze_me=True,variable_names=[factor_name])[factor_name]
                try:
                    iData=iData[iData[:,2]==factor_id]
                except(IndexError):
                    continue
                windcodes=[x[:-3] for x in CodeConvertStr(iData[:,1])]
                iData=pd.DataFrame(iData[:,-1].reshape((1,-1)),index=[idate],columns=windcodes)
                Rslt=Rslt.append(iData)
        elif dates is not None:
            for idate in dates:
                if idate in allDates:
                    print('正在读取【%s】数据'%idate)
                    iData=sio.loadmat(tablePath+os.sep+idate+'.mat',squeeze_me=True,variable_names=[factor_name])[factor_name]
                    try:
                        iData=iData[iData[:,2]==factor_id]
                    except(IndexError):
                        continue                    
                    windcodes=[x[:-3] for x in CodeConvertStr(iData[:,1])]
                    iData=pd.DataFrame(iData[:,-1].reshape((1,-1)),index=[idate],columns=windcodes)
                    Rslt=Rslt.append(iData)
        else:
            dates=[x for x in allDates if x>=start_date and x<=end_date]
            for idate in dates:
                print('正在读取【%s】数据'%idate)
                iData=sio.loadmat(tablePath+os.sep+idate+'.mat',squeeze_me=True,variable_names=[factor_name])[factor_name]
                try:
                    iData=iData[iData[:,2]==factor_id]
                except(IndexError):
                    continue
                windcodes=[x[:-3] for x in CodeConvertStr(iData[:,1].astype('int'))]
                iData=pd.DataFrame(iData[:,-1].reshape((1,-1)),index=[idate],columns=windcodes)
                iData=iData.loc[:,~iData.columns.duplicated()]
                Rslt=Rslt.append(iData)
        self.DataLock.release()
        if ids is not None:
            Rslt=Rslt.ix[:,ids]
        Rslt.sort_index(axis=1,inplace=True)
        Rslt=Rslt.copy()
        gc.collect()
        return Rslt
    #获取某张表的因子名称
    def getFieldName(self,table_name,ignore=[]):
        tablePath = self.getTablePath(table_name)
        allDates = self.getTableDate(table_name)
        oneDate = allDates[0]
        data = sio.loadmat(tablePath+os.sep+oneDate+'.mat',squeeze_me=True)['output']
        factorIDs = np.unique(data[:,2])
        return list(factorIDs.astype('int32'))
#sqLite3DB
class sqlLiteDB(object):
    def __init__(self,dir_path=sqliteDB,lock=Lock()):
        self.DirPath=dir_path
        self.Lock=lock
        self.updateInfo()
    #列出数据库中的表名以及对应字段名
    def updateInfo(self):
        allFiles=os.listdir(self.DirPath)
        allDBs=[x[:-3] for x in allFiles if x[-3:]=='.db']
        tableFactor={}
        for iDB in allDBs:
            conn=sqlite3.connect(self.getDBPath(iDB))
            sql_show_tables="SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
            allTables=pd.read_sql(sql_show_tables,conn)
            sql_table_info="PRAGMA table_info(%s)"%allTables.ix[0,'name']
            allFileds=pd.read_sql(sql_table_info,conn)
            tableFactor[iDB]={allTables.ix[0,'name']:allFileds['name'].tolist()}
            conn.close()
        self.TableFactor = tableFactor
        return 1
    #检查表是否存在
    def checkTableExistence(self,table_name):
        return table_name in self.TableFactor
    #获得某个数据库路径
    def getDBPath(self,db_name):
        return self.DirPath+os.sep+db_name+'.db'
    #执行查询于某个数据库
    def execSQLQuery(self,sql,db_name):
        conn=sqlite3.connect(self.getDBPath(db_name))
        rslt=pd.read_sql(sql,conn)
        conn.close()
        return rslt
    #获取某张表的因子数据
    def loadFactor(self,db_name,factors=None,ids=None,dates=None,start_date=None,end_date=None,date_col=None):
        if not self.checkTableExistence(db_name):
            raise KeyError("%s not exist !"%db_name)
        conn=sqlite3.connect(self.getDBPath(db_name))
        sql_show_tables="SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
        allTables=pd.read_sql(sql_show_tables,conn)
        date_col = 'Dates' if date_col is None else date_col
        if factors is None:
            sql_query="SELECT * FROM "+allTables.ix[0,'name']
        else:       
            sql_query="SELECT "+",".join(factors)+" FROM "+allTables.ix[0,'name']
        if ids is not None:
            sql_query+=" WHERE IDs IN ('"+"','".join(ids)+"')"
        if dates is not None:
            if ids is None:
                sql_query+=" WHERE %s IN ("%date_col+",".join(dates)+")"
            else:
                sql_query+=" AND %s IN ("%date_col+",".join(dates)+")"
        elif (start_date is not None) and (end_date is not None):
            if ids is not None:
                sql_query+=" AND %s BETWEEN %s AND %s"%(date_col,start_date,end_date)
            else:
                sql_query+=" WHERE %s BETWEEN %s AND %s"%(date_col,start_date,end_date)
        elif (start_date is None) and (end_date is not None):
            if ids is not None:
                sql_query+=" AND %s <= %s"%(date_col,end_date)
            else:
                sql_query+=" WHERE %s <= %s"%(date_col,end_date)
        elif (start_date is not None) and (end_date is None):
            if ids is not None:
                sql_query+=" AND %s >= %s"%(date_col,start_date)
            else:
                sql_query+=" WHERE %s >= %s"%(date_col,start_date)
        rslt=pd.read_sql(sql_query,conn)
        conn.close()
        rslt = format_column_names(rslt)
        return rslt
    #加载单因子，返回ShelveDB格式
    def loadSingleFactor(self,db_name,factors,ids=None,dates=None,start_date=None,end_date=None):
        conn=sqlite3.connect(self.getDBPath(db_name))
        sql_show_tables="SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
        allTables=pd.read_sql(sql_show_tables,conn)
        if isinstance(factors,list):
            factors = [factors]
        if 'IDs' not in factors:
            factors += ['IDs']
        if 'Dates' not in factors:
            factors += ['Dates']

        sql_query="SELECT "+",".join(factors)+" FROM "+allTables.ix[0,'name']
        if ids is not None:
            sql_query+=" WHERE IDs IN ('"+"','".join(ids)+"')"
        if dates is not None:
            if ids is None:
                sql_query+=" WHERE Dates IN ("+",".join(dates)+")"
            else:
                sql_query+=" AND Dates IN ("+",".join(dates)+")"
        elif (start_date is not None) and (end_date is None):
            if ids is not None:
                sql_query+=" AND Dates >= %s"%start_date
            else:
                sql_query+=" WHERE Dates >= %s"%start_date
        elif (start_date is None) and (end_date is not None):
            if ids is not None:
                sql_query+=" AND Dates <= %s"%end_date
            else:
                sql_query+=" WHERE Dates <= %s"%end_date
        else:
            if ids is not None:
                sql_query+=" AND Dates BETWEEN %s AND %s"%(start_date,end_date)
            else:
                sql_query+=" WHERE Dates BETWEEN %s AND %s"%(start_date,end_date)
        rslt=pd.read_sql(sql_query,conn)
        rslt = rslt.set_index(['Dates','IDs']).unstack()
        conn.close()
        return rslt
    
    #存储数据
    def saveFactors(self,data,db_name,table_name=None,if_exists='append',prime_keys=None):
        isExist = self.checkTableExistence(db_name)
        if (not isExist):
            conn=sqlite3.connect(self.getDBPath(db_name))
            data.to_sql(table_name,conn,index=False)
            conn.close()
        else:
            conn=sqlite3.connect(self.getDBPath(db_name))
            sql_show_tables="SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
            allTables=pd.read_sql(sql_show_tables,conn)
            data.to_sql(allTables.ix[0,'name'],conn,if_exists=if_exists,index=False)
            conn.close()
        self.updateInfo()
        return 1

    #某个数据库的所有表
    def getTableNames(self, database):
        conn = sqlite3.connect(self.getDBPath(database))
        sql_show_tables="SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
        allTables=pd.read_sql(sql_show_tables,conn)
        conn.close()
        return allTables.ix[:, 'name'].tolist()

    #给每一张表插入自增序列
    def insertRowID(self, database):
        tables = self.getTableNames(database)
        conn = sqlite3.connect(self.getDBPath(database))
        for table in tables:
            data = pd.read_sql("SELECT * FROM %s"%table, conn)
            if 'rowids' not in data.columns:
                data['rowids'] = np.arange(1, len(data)+1)
                data.to_sql(table, conn, if_exists='replace', index=False)
        conn.close()
        return 1

    #给某张表去重
    def dropDuplicates(self, database, keys):
        tables = self.getTableNames(database)
        conn = sqlite3.connect(self.getDBPath(database))
        for table in tables:
            sql_drop_dp = "DELETE FROM " + table
            sql_drop_dp += " WHERE ROWID NOT IN ( SELECT MAX(ROWID) FROM "
            sql_drop_dp += table + " GROUP BY " + ",".join(keys) + ")"

            conn.execute(sql_drop_dp)
        conn.commit()
        conn.close()
        return 1

    #合并两个表
    def mergeTable(self,table1,table2,new_table=False,new_table_name=None):
        data1=self.loadFactor(table1)
        data2=self.loadFactor(table2)
        conn1=sqlite3.connect(self.getDBPath(table1))
        data=data1.append(data2).reset_index(drop=True)
        sql_show_tables="SELECT NAME FROM SQLITE_MASTER WHERE TYPE='table'"
        allTables=pd.read_sql(sql_show_tables,conn1)
        if new_table:
            conn2=sqlite3.connect(self.getDBPath(new_table_name))
            if new_table_name is None:
                raise ValueError("表名不能为空")
            data.to_sql(allTables.ix[0,'name'],conn2,if_exists='replace',index=False)
            conn2.close()
        else:
            data.to_sql(allTables.ix[0,'name'],conn1,if_exists='replace',index=False)
        conn1.close()
        self.TableFactor=self.updateInfo()
        return 1

    #创建表
    def createTable(self, table_name):
        if self.checkTableExistence(table_name):
            raise ValueError("该表已存在，不能创建！")
        tablePath = self.getDBPath(table_name)
        conn = sqlite3.connect(tablePath)
        conn.close()
        self.TableFactor[table_name]={}
        return 1

    #计算某张表的最大日期
    def getMaxDate(self, table_name, date_col='Dates'):
        field_name = list(self.TableFactor[table_name])[0]
        SQLStr = 'select max(' + date_col + ')'
        SQLStr += ' from ' + field_name

        data = self.execSQLQuery(SQLStr, table_name)

        return data.iloc[0, 0]

def format_column_names(data):
    '''重命名数据的列名，方便调用
       主要规则如下：
            1.证券代码列命名为'IDs'
            2.日期列命名为'Date'
            3.因子列名都是小写
    '''
    columns = data.columns.str.lower()
    columns = ['IDs' if x in ['ids','windcode'] else x for x in columns]
    columns = ['Date' if x in ['date','dates','trade_dt'] else x for x in columns]
    data.columns = columns

    return data