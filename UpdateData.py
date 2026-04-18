#--*-- coding:utf-8 --*--import os
import sqlite3
import sys
import argparse
import time
import shutil
from pathlib import Path
# Path("parent_folder/new_folder").mkdir(parents=True, exist_ok=True, mode=0o777) #创建两级目录，可设置目录权限
'''time.time(): 这个函数返回当前时间的时间戳（自1970年1月1日午夜（UTC/GMT的午夜）以来的秒数）。
time.localtime(time.time()): 这个函数将时间戳转换为本地时间的struct_time。它返回一个包含年、月、日等信息的结构化时间对象。
time.strftime('%H:%M:%S', ...)): 这个函数通过格式化字符串将struct_time对象转换为字符串。'%H:%M:%S' 是格式化字符串，表示小时:分钟:秒。这会将struct_time对象中的时间部分格式化成这种形式。
str(...).replace(':', '_'): 这一部分先将格式化后的时间字符串转换为字符串，然后使用 replace() 方法将其中的冒号（:）替换为下划线（_）。'''
curtime = str(time.strftime('%Y-%m-%d %H:%M',time.localtime(time.time()))) #获取本地时间
curtime2 = str(time.strftime('%H:%M:%S',time.localtime(time.time()))).replace(':', '_')# 转换成时分秒并替换字符
print(curtime2) # 显示 时-分-秒

NEWTIME = curtime.replace(' ', '').replace(':', '').replace('-', '') # 替换字符，去掉无用的字符。

print(NEWTIME) # 显示年-月-日-时-分-秒

NECURNAME = '6_23_10_09_02_xformat_202311041105_PatchInfo_DefectInfo_' + NEWTIME + '_TSET_' + curtime2 # 组合信息。

# ''' REST = "": 初始化一个字符串 REST，用于保存提取的文件名。
#
# RESTINDEX = -1: 初始化一个变量 RESTINDEX，用于追踪当前处理的文件在列表中的索引。初始值设为 -1，因为在下面的循环开始前，会先自增一次。
#
# for y in data:: 遍历传入的列表 data 中的每一个元素，这里假设 data 是包含文件路径的列表。
#
# RESTINDEX += 1: 在每次循环开始时，RESTINDEX 自增。这是为了记录当前处理的文件在列表中的索引。
#
# if RESTINDEX != 0:: 这个条件检查，确保在第一个文件名之前不会添加逗号。因为第一个文件名前不需要逗号分隔。
#
# filename, ext = os.path.splitext(os.path.basename(y)): 使用 os.path 模块从文件路径 y 中提取文件名（不包含路径）和文件扩展名。
#
# REST = REST + ', ' + filename: 将提取的文件名添加到 REST 字符串中，用逗号和空格分隔。
#
# 最终，REST 字符串将包含 data 列表中所有文件的文件名，用逗号分隔。这样的字符串可能用于某些需要文件名列表的操作，比如生成一个包含这些文件名的命令行参数字符串。'''
def CODES(data):
    REST = ""
    RESTINDEX = -1
    for y in data:
        RESTINDEX += 1
        if RESTINDEX != 0:
            REST = REST + ','
        filename, ext = os.path.splitext(os.path.basename(y))
        REST = REST + filename
    # print(REST)
    return REST

def idsCODES(data):
    REST = ""
    RESTINDEX = -1
    for y in data:
        RESTINDEX += 1
        if RESTINDEX != 0:
            REST = REST + ','
        REST = REST + str(y[0])
    # print(REST)
    return REST

def valuesCODES(data):
    REST = ""
    RESTINDEX = -1
    for y in data:
        RESTINDEX += 1
        if RESTINDEX != 0:
            REST = REST + ','
        REST = REST + '?'
   # print(REST)
    return REST

def MDFILE(data):
    RESTINDEX = -1
    for y in data:
        # print(y[1])
        RESTINDEX += 1
        Path(outPagesPath + "/" + NECURNAME + "/" + str(y[1])).mkdir(parents=True, exist_ok=True, mode=0o777)
    return RESTINDEX


def COPYFILE(data,oldPath):
    
    RESTINDEX = -1
    for y in data:
        # print(y)
        RESTINDEX += 1
        shutil.copy2(oldPath + '\\' + str(y[1]) + '\\' +  str(y[0]) + '.sq3', outPagesPath + '\\' + NECURNAME + '\\' + str(y[1])+ '\\'+  str(y[0]) + '.sq3') #复制src_folder目录到dst_folder目录
    return RESTINDEX


if getattr(os.sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(os.sys.executable))
elif __file__:
    absPath = os.path.dirname(os.path.abspath(__file__))

allPagesPath = os.path.join(absPath, "all")
inPagesPath = os.path.join(absPath, 'in')
outPagesPath = os.path.join(absPath, 'out')
if os.path.exists(inPagesPath) == False:
    os.mkdir(inPagesPath)
    print(inPagesPath) # 显示年-月-日-时-分-秒
if os.path.exists(outPagesPath) == False:
    os.mkdir(outPagesPath)
    print(outPagesPath) # 显示年-月-日-时-分-秒

prcess_table_dict = {
    "Road" : ["R_ID",1],
    "Lane" : ["L_ID",2],
    "LaneMarking" : ["LM_ID",3],
    "JunctionArea" : ["JA_ID",6],
    "RoadMark" : ["RM_ID",7],
    "TrafficSign" : ["TS_ID",8],
    "TrafficLight" : ["TL_ID",9],
    "Pole" : ["POLE_ID",10],
    "StopLine" : ["SL_ID",12],
    "PolygonalFacility" : ["PF_ID",17],
    # "RestrictionLine" ：[]
}


# 修改
def edit(old,new,ids,layer,table):
    # 1.删除new里ids数据
    # 2.赋值old里ids数据到new
    # 3.车道边线直接删除新增 车道删除新增加修改关联表 道路直接删除新增
    # 
    oldconn = sqlite3.connect(old)
    oldcur = oldconn.cursor()
    oldsql = "SELECT * FROM patchinfo where PCINF_TYPE = 4 and layer = " + layer
    oldcur.execute(oldsql)
    oldconn.commit()
    oldconn.close()

    newconn = sqlite3.connect(new)
    newcur = newconn.cursor()
    newsql = "SELECT * FROM patchinfo where PCINF_TYPE = 4 and layer = " + layer
    newcur.execute(newsql)
    newconn.commit()


    newconn.close()
    # print(cur.fetchall())

# 根据删除road删除路肩
# 根据新增road复制路肩

def getNotNew(new,table,id_filed,ids):
    newconn = sqlite3.connect(new)
    newcur = newconn.cursor()
    newcur.execute("SELECT {} from {} where {} in ( {} )".format(id_filed,table,id_filed,ids))
    newids = newcur.fetchall()
    for id in ids.split(','):
        # print('---log',new + ' ' + table + ' '+ id_filed + ' '+ str(id))
        if str(id) not in idsCODES(newids).split(','):
            print('---err',new + ' ' + table + ' '+ id_filed + ' '+ str(id))
    newconn.close()

# 删除
def dels(new,table,id_filed,ids):
    # print(new,table,id_filed,ids)
    newconn = sqlite3.connect(new)
    newcur = newconn.cursor()

    getNotNew(new,table,id_filed,ids)
    # return
    newsql = "delete from {} where {} in ( {} )".format(table,id_filed,ids)
    newcur.execute(newsql)

    # print(newsql)
    if id_filed == 'L_ID':
        newsql2 = "delete from LaneSplitMerge where {} in ( {} )".format(id_filed,ids)
        newsql3 = "delete from LaneTypeInfo where {} in ( {} )".format(id_filed,ids)
        newsql4 = "delete from LaneWidthInfo where {} in ( {} )".format(id_filed,ids)
        newsql5 = "delete from RestrictionLine where {} in ( {} )".format(id_filed,ids)
        newcur.execute(newsql2)
        newcur.execute(newsql3)
        newcur.execute(newsql4)
        newcur.execute(newsql5)

    newconn.commit()
    newconn.close()
# 新增
def adds(new,table,Columns,values,data):
    # print(new,table,Columns,values,data)
    newconn = sqlite3.connect(new)
    newcur = newconn.cursor()
    sql = "INSERT INTO {} ( {} ) VALUES ( {} )".format(table,Columns,values)
    # print(sql)
    # values = [(item['field1'], item['field2'], item['field3']) for item in data]
    newcur.executemany(sql, data)
    # if table == 'Lane':
    #     oldconn = sqlite3.connect(old)
    #     oldcur = oldconn.cursor()
    #     newsql2 = "select * from LaneSplitMerge where L_ID in ( {} )".format(ids)

    #     newcur.execute(newsql2)
    #     newcur.execute(newsql3)
    #     newcur.execute(newsql4)
    #     newcur.execute(newsql5)
    newconn.commit()
    newconn.close()
    

def get_all_files(file_path,file_list,file_filter=".sq3"):
    for file_name in os.listdir(file_path):
        file = os.path.join(file_path,file_name)
        if os.path.isdir(file):
            get_all_files(file,file_list,file_filter)
        else:
            if os.path.basename(file).endswith(file_filter) :
                print(file)
                file_list.append(file)

def gettableinfo(cur=sqlite3.Cursor):
    cur.execute("SELECT name from sqlite_master WHERE TYPE = 'table'")
    table_info = []
    for row in cur.fetchall():
        table_info.append(row[0])
    return table_info

def check_column_exists(cursor=sqlite3.Cursor,table_name=str(), column_name=str()):
    cursor.execute("PRAGMA table_info({})".format(table_name))
    columns = [column[1].upper() for column in cursor.fetchall()]
    return column_name in columns

def getColumns(Columns):
    # Columns = (('PCINF_ID', None, None, None, None, None, None), ('FEATURE_ID', None, None, None, None, None, None), ('ISSUE_TYPE', None, None, None, None, None, None), ('PCINF_TYPE', None, None, None, None, None, None))
    nameArr = []
    for y in Columns:
        nameArr.append(y[0]) 
        # print(nameArr)
    return nameArr
def ColumnsCODES(data):
    REST = ""
    RESTINDEX = -1
    for y in data:
        RESTINDEX += 1
        if RESTINDEX != 0:
            REST = REST + ','
        REST = REST + y 
   # print(REST)
    return REST

def ProcessOneFile(filepath=str,filename=str):
    tilecur.execute('SELECT TILE_ID,code FROM Tile WHERE TILE_ID = {}'.format(filename))
    data = tilecur.fetchall()
    newPath = outPagesPath + "\\" + NECURNAME + "\\"  + str(data[0][1]) + "\\" + filename + '.sq3'
    print("Begin process file:",filepath)
    conn = sqlite3.connect(filepath)
    cur = conn.cursor()

    table_name = gettableinfo(cur)
    # sql = "attach '" + filename + "' as src;"
    # cur.execute(sql)
    print(table_name)
    # return
    patchinfosqls = "SELECT * FROM patchinfo"
    cur.execute(patchinfosqls)
    patchinfodata = cur.fetchall()
    # print(data)
    myColumns = getColumns(cur.description)
    adds(newPath,'patchinfo',ColumnsCODES(myColumns),valuesCODES(myColumns),patchinfodata)

    newconn = sqlite3.connect(filepath)
    newcur = conn.cursor()


    for table in  table_name:
        #print("table name =",table)
        if table not in prcess_table_dict.keys():
            continue
        layerid = str(prcess_table_dict[table][1])
        id_filed = str(prcess_table_dict[table][0])
        
        print("Process table",table)
        
        sql = "SELECT FEATURE_ID FROM patchinfo where PCINF_TYPE = 5 and layer = " + layerid
        cur.execute(sql)
        patchinfoids = cur.fetchall()
        if len(patchinfoids) != 0:
            # print("Process patchinfoids---",idsCODES(patchinfoids))
            dels(newPath,table,id_filed,idsCODES(patchinfoids))

            # 删除道路是将相关路肩删掉
            if layerid == '1':
                print('--')
                cur.execute("SELECT L_ID FROM Lane where R_ID in ( {} ) and L_TYPE = 65536".format(idsCODES(patchinfoids)))
                data = cur.fetchall()
                dels(newPath,'Lane','L_ID',idsCODES(data))



        sql2 = "SELECT FEATURE_ID FROM patchinfo where PCINF_TYPE = 4 and layer = " + layerid
        cur.execute(sql2)
        patchinfoids4 = cur.fetchall()
        ids4 = idsCODES(patchinfoids4)
        if len(patchinfoids4) != 0:
            dels(newPath,table,id_filed,ids4)
            sqls = "SELECT * FROM {} where {} in ( {} )".format(table,id_filed,ids4)
            cur.execute(sqls)
            data = cur.fetchall()
            myColumns = getColumns(cur.description)
            adds(newPath,table,ColumnsCODES(myColumns),valuesCODES(myColumns),data)
            if layerid == '2':
                    cur.execute("SELECT * FROM LaneSplitMerge where L_ID in ( {} )".format(ids4))
                    data = cur.fetchall()
                    myColumns = getColumns(cur.description)
                    adds(newPath,'LaneSplitMerge',ColumnsCODES(myColumns),valuesCODES(myColumns),data)
                    cur.execute("SELECT * FROM LaneTypeInfo where L_ID in ( {} )".format(ids4))
                    data = cur.fetchall()
                    myColumns = getColumns(cur.description)
                    adds(newPath,'LaneTypeInfo',ColumnsCODES(myColumns),valuesCODES(myColumns),data)
                    cur.execute("SELECT * FROM LaneWidthInfo where L_ID in ( {} )".format(ids4))
                    data = cur.fetchall()
                    myColumns = getColumns(cur.description)
                    adds(newPath,'LaneWidthInfo',ColumnsCODES(myColumns),valuesCODES(myColumns),data)
                    cur.execute("SELECT * FROM RestrictionLine where L_ID in ( {} )".format(ids4))
                    data = cur.fetchall()
                    myColumns = getColumns(cur.description)
                    adds(newPath,'RestrictionLine',ColumnsCODES(myColumns),valuesCODES(myColumns),data)

        sql3 = "SELECT FEATURE_ID FROM patchinfo where PCINF_TYPE = 6 and layer = " + layerid
        cur.execute(sql3)
        patchinfoids6 = cur.fetchall()
        ids6 = idsCODES(patchinfoids6)
        if len(patchinfoids6) != 0:
            sqls2 = "SELECT * FROM {} where {} in ( {} )".format(table,id_filed,ids6)
            cur.execute(sqls2)
            data = cur.fetchall()
            myColumns = getColumns(cur.description)
            adds(newPath,table,ColumnsCODES(myColumns),valuesCODES(myColumns),data)


            if layerid == '1':
                cur.execute("SELECT * FROM Lane where R_ID in ( {} ) and L_TYPE = 65536".format(idsCODES(patchinfoids6)))
                Lanedata = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'Lane',ColumnsCODES(myColumns),valuesCODES(myColumns),Lanedata)
                cur.execute("SELECT * FROM LaneTypeInfo where L_ID in ( {} )".format(idsCODES(Lanedata)))
                ltdata = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'LaneTypeInfo',ColumnsCODES(myColumns),valuesCODES(myColumns),ltdata)

                cur.execute("SELECT * FROM LaneWidthInfo where L_ID in ( {} )".format(idsCODES(Lanedata)))
                lwdata = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'LaneWidthInfo',ColumnsCODES(myColumns),valuesCODES(myColumns),lwdata)


            # 
            if layerid == '2':
                print('---------layerid')
                cur.execute("SELECT * FROM LaneSplitMerge where L_ID in ( {} )".format(ids6))
                data = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'LaneSplitMerge',ColumnsCODES(myColumns),valuesCODES(myColumns),data)
                cur.execute("SELECT * FROM LaneTypeInfo where L_ID in ( {} )".format(ids6))
                data = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'LaneTypeInfo',ColumnsCODES(myColumns),valuesCODES(myColumns),data)
                cur.execute("SELECT * FROM LaneWidthInfo where L_ID in ( {} )".format(ids6))
                data = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'LaneWidthInfo',ColumnsCODES(myColumns),valuesCODES(myColumns),data)
                cur.execute("SELECT * FROM RestrictionLine where L_ID in ( {} )".format(ids6))
                data = cur.fetchall()
                myColumns = getColumns(cur.description)
                adds(newPath,'RestrictionLine',ColumnsCODES(myColumns),valuesCODES(myColumns),data)

        sql4 = "SELECT FEATURE_ID FROM patchinfo where PCINF_TYPE = 1 and layer = " + layerid
        cur.execute(sql4)
        patchinfoids1 = cur.fetchall()
        getNotNew(newPath,table,id_filed,idsCODES(patchinfoids1))
        # print("Process myColumns",myColumns)
        
    # cur.execute("detach src;")
    conn.commit()
    conn.close()
    print("End process file:",filepath)

# if __name__ == '__main__':

#     parser = argparse.ArgumentParser()
#     parser.add_argument("--datapath",type=str,required=True)
#     parser.add_argument("--filename",type=str,required=True)
#     args = parser.parse_args()
#     db_file_list = []
#     get_all_files(args.datapath,db_file_list,".sq3")
#     for db_file in db_file_list:
#         ProcessOneFile(db_file,args.filename)
#     print("Process all files finished!")
# newconn = sqlite3.connect(new)
# newcur = newconn.cursor()
# newsql = "SELECT * FROM patchinfo where PCINF_TYPE = 4 and layer = " + layer
# newcur.execute(newsql)
# newconn.commit()
tileconn = sqlite3.connect(r'D:\0----guoxiaohui\6_23_10_09_02_Tile.sq3')
tilecur = tileconn.cursor()

db_file_list = []
get_all_files('D:\\0----guoxiaohui\\odd-work-data-10\\11_10\\test_2\\新建文件夹\\',db_file_list,".sq3")

tilecur.execute('SELECT TILE_ID,code FROM Tile WHERE TILE_ID IN ( + ' + CODES(db_file_list)+') group by code')
data = tilecur.fetchall()
MDFILE(data)
tilecur.execute('SELECT TILE_ID,code FROM Tile WHERE TILE_ID IN ( + ' + CODES(db_file_list)+')')
data2 = tilecur.fetchall()
COPYFILE(data2,'D:\\0----guoxiaohui\\6_23_10_09_02_all_xformat_202310201110_202311041105\\xformat_db')
for db_file in db_file_list:
    # 6_23_10_09_02_Tile.sq3
    filename, ext = os.path.splitext(os.path.basename(db_file))
    # print(filename,ext)
    ProcessOneFile(db_file,filename)


# if 1 not in [1,2]:
#     print('----')
    
