import os
import sqlite3
import time
import argparse
curtime = str(time.strftime('%Y-%m-%d %H:%M',time.localtime(time.time())))
curtime2 = str(time.strftime('%H:%M:%S',time.localtime(time.time()))).replace(':', '_')

if getattr(os.sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(os.sys.executable))
elif __file__:
    absPath = os.path.dirname(os.path.abspath(__file__))


def get_all_files(file_path,file_list,file_filter=".sq3"):
    for file_name in os.listdir(file_path):
        file = os.path.join(file_path,file_name)
        if os.path.isdir(file):
            get_all_files(file,file_list,file_filter)
        else:
            if os.path.basename(file).endswith(file_filter) :
                # print(file)
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
    # print(table_name,column_name,columns,column_name)

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

def valuesCODES(data):
    REST = ""
    RESTINDEX = -1
    for y in data:
        RESTINDEX += 1
        if RESTINDEX != 0:
            REST = REST + ','
        REST = REST + '?'
   # print(REST)
    return

def adds_table(table,Columns,values,add_data,main_cur=sqlite3.Cursor):
    # print('新增数据',table,idsCODES(add_data))
    sql = "INSERT INTO {} ( {} ) VALUES ( {} )".format(table,Columns,values)
    # print(sql)
    # values = [(item['field1'], item['field2'], item['field3']) for item in data]
    main_cur.executemany(sql, add_data)


if __name__ == "__main__" :
    
    base_xf = "base_xf.sq3"
    base_conn = sqlite3.connect(base_xf)
    base_conn.text_factory = lambda x : str(x, 'gbk', 'ignore')
    base_cur = base_conn.cursor()
    base_cur.execute("SELECT name, sql from sqlite_master WHERE TYPE = 'table'")
    basetableinfo = []
    base_table_dict = {}
    for row in base_cur.fetchall():
        basetableinfo.append(row[0])
        base_table_dict[row[0]] = {
            "sql": row[1]
        }
        base_cur.execute(f"PRAGMA table_info({row[0]});")
        fileds = {}
        for filedrow in base_cur.fetchall():
            # print(filedrow)
            fileds[filedrow[1]] = {
                "name":filedrow[1],
                "type":filedrow[2],
                "notnull":filedrow[3],
            }
        base_table_dict[row[0]]["fileds"] = fileds
    # print(base_table_dict)
    parser = argparse.ArgumentParser()
    parser.add_argument("--datapath",type=str,required=True)
    args = parser.parse_args()
    branch_dir = args.datapath # '/home/root123/data/gxh/6_24_06_29_02_xformat_202406302346_PatchInfo_DefectInfo_202407071200_pure'
    data_file_list = []
    get_all_files(branch_dir,data_file_list,".sq3")
    up_tile = []
    # 遍历db_file_list 单独处理作业图幅
    for db_file in data_file_list:
        # 获取路径的文件名
        filename, ext = os.path.splitext(os.path.basename(db_file))
        main_conn = sqlite3.connect(db_file)
        main_conn.text_factory = lambda x : str(x, 'gbk', 'ignore')
        main_cur = main_conn.cursor()
        tableinfo = gettableinfo(main_cur)
        # 需要添加表
        base_tables = base_table_dict.keys()
        diffadd = list(set(base_tables) - set(tableinfo))    
        diffdel = list( set(tableinfo) - set(base_tables))
        # 删除多余图层
        for table in diffdel:
            main_cur.execute(f"DROP TABLE IF EXISTS {table};")
            print(f"删除多余图层{filename} {table}")
        # print(diffadd,diffdel)

        # # 新增缺失图层并检查已有图层是否和基准字段一致，
        for table in base_tables:
            table_config = base_table_dict[table]
            if table in diffadd:
                main_cur.execute(table_config["sql"])
                print(f"新增缺失图层{filename} {table}")
            else:
                main_cur.execute(f"PRAGMA table_info({table});")
                fileds = {}
                for filedrow in main_cur.fetchall():
                    # print(filedrow)
                    fileds[filedrow[1]] = {
                        "name":filedrow[1],
                        "type":filedrow[2],
                        "notnull":filedrow[3],
                    }
                # print('b',table_config["fileds"].keys())
                # print('f',fileds.keys())
                filedadd = list(set(table_config["fileds"].keys()) - set(fileds.keys()))    
                # print('d',filedadd)
                fileddel = list( set(fileds.keys()) - set(table_config["fileds"].keys()))
                for delfiled in fileddel:
                    # print("删除",f"ALTER TABLE {table} DROP COLUMN '{delfiled}';")
                    main_cur.execute(f"ALTER TABLE {table} DROP COLUMN '{delfiled}';")
                    print(f"删除图层字段{filename} {table} {delfiled}")
                for addfiled in filedadd:
                    filedinfo = table_config["fileds"][addfiled]
                    main_cur.execute(f"ALTER TABLE {table} ADD COLUMN '{addfiled}' {filedinfo['type']};")
                    print(f"新增图层字段{filename} {table} {addfiled}")
                    
                # 检查字段规格
                # print(table)
        # print(base_tables)
        if "geometry_columns" in base_tables:
                 main_cur.execute(f"ATTACH DATABASE '{base_xf}' AS database_alias;")
                 in_sql = "INSERT INTO geometry_columns SELECT * FROM database_alias.geometry_columns as alias_columns where alias_columns.f_table_name not in (SELECT f_table_name FROM geometry_columns);"
                 main_cur.execute(in_sql)

            # print(table)
        # print(tableinfo)
        # ALTER TABLE SuggestPath_LOG ADD COLUMN 'check_type' BIGINT;
        # if 'SuggestPath' not in tableinfo:
        #     print(f"SuggestPath 不在 {db_file} 里")
        #     main_conn.close()
        #     continue
        # if 'SDRoad' not in tableinfo:
        #     print(f"SDRoad 不在 {db_file} 里")
        #     main_conn.close()
        #     continue
        # if check_column_exists(main_cur,'SuggestPath','TURN_FLAG') == True:
        #     # print(filename)
        #     main_cur.execute(f"ALTER TABLE SuggestPath DROP COLUMN TURN_FLAG;")
        #     main_conn.commit()
        # if check_column_exists(main_cur,'SDRoad','TO_ANGLE') == True:
        #     # print(filename)
        #     main_cur.execute(f"ALTER TABLE SDRoad DROP COLUMN TO_ANGLE;")
        #     main_conn.commit()
        # if check_column_exists(main_cur,'SDRoad','FROM_ANGLE') == True:
        #     # print(filename)
        #     main_cur.execute(f"ALTER TABLE SDRoad DROP COLUMN FROM_ANGLE;")
        #     main_conn.commit()
        main_conn.commit()    
        main_conn.close()
        

# python upfieldvalue.py --datapath=D:\code\python\新建文件夹 --tablename=Lane --fieldname=L_ID --startnum=100000000 --where="L_TYPE = 65536"
# python 更新图层主键.py --datapath=D:\code\python\新建文件夹 --tablename=Lane --fieldname=L_ID --startnum=1 --where="L_TYPE = 65536"