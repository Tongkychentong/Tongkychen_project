#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File    :   run_centerline_patch.py
@Time    :   2024/06/28 10:13:57
@Author  :   zoushuquan@xiaomi.com 
@Version :   1.0
@Desc    :   None
@Note    :   None
'''

import os
import argparse
import json
import time 
import sqlite3
import pandas as pd
import requests  
curtime = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
curtime = curtime.replace(' ', '__')
curtime = curtime.replace(':', '-')

if getattr(os.sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(os.sys.executable))
elif __file__:
    absPath = os.path.dirname(os.path.abspath(__file__))


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

def get_data(table,id_filed,ids,cur=sqlite3.Cursor,test = ""):
    # print(table,id_filed,ids,test)
    cur.execute("SELECT * FROM {} where {} in ( {} ) {}".format(table,id_filed,ids,test))
    data = cur.fetchall()
    myColumns = getColumns(cur.description)

    return [data,myColumns]
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
def adds_table(table,Columns,values,add_data,main_cur=sqlite3.Cursor):
    # print('新增数据',table,idsCODES(add_data))
    sql = "INSERT INTO {} ( {} ) VALUES ( {} )".format(table,Columns,values)
    # print(sql)
    # values = [(item['field1'], item['field2'], item['field3']) for item in data]
    main_cur.executemany(sql, add_data)

    
def put_colour(txt,color=None):
    if color == 'red':
        result = f"\033[31m{txt}\033[0m"
    elif color == 'green':
        result = f"\033[32m{txt}\033[0m"
    elif color == 'yellow':
        result = f"\033[33m{txt}\033[0m"
    elif color == 'blue':
        result = f"\033[34m{txt}\033[0m"
    elif color == 'violet':
        result = f"\033[35m{txt}\033[0m"
    elif color == 'cyan':
        result = f"\033[36m{txt}\033[0m"
    elif color == 'gray':
        result = f"\033[37m{txt}\033[0m"
    elif color == 'black':
        result = f"\033[30m{txt}\033[0m"
    else:
        result = txt
    return result
    
class MyException(Exception):
    pass

def get_all_files(file_path,file_list,file_filter=".sq3"):
    for file_name in os.listdir(file_path):
        file = os.path.join(file_path,file_name)
        if os.path.isdir(file):
            get_all_files(file,file_list,file_filter)
        else:
            if os.path.basename(file).endswith(file_filter) :
                file_list.append(file)
        
def suplist(data):
    li = []
    if len(data) == 0:
        return []
    for row in data:
        li.append(row[0])
    return li

def sendMsg(content,title=""):
    s = json.dumps({
        "msg_type": "post", "content": {
            "post": {
                "zh_cn": {
                    "title": title, # ,
                    "content": [
                        [{
                            "tag": "text",
                            "text": content
                        }]
                    ]
                }
            }
        }
    })
    requests.post(f'https://open.f.mioffice.cn/open-apis/bot/v2/hook/e18a5ae0-8233-455f-9f81-ae8a2a07d6a8',data=s)


    
def getLmDataTile(lm_id,db_file_list):
    cur_filename = None
    for file_path in db_file_list:
        filename, ext = os.path.splitext(os.path.basename(file_path))
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        # 使用自定义函数进行替换
        cursor.execute(f"SELECT rowid,TILE,LM_ID FROM LaneMarking where LM_ID={lm_id}")
        data = cursor.fetchone()
        conn.close()
        if data is not None:
            cur_filename = filename
            break
    return cur_filename
        

def find_roads_by_ids(all_roads, f_r_ids):
    """
    从 all_roads 中查找 f_r_ids 对应的道路信息

    :param all_roads: 所有道路信息列表
    :param f_r_ids: 需要查找的道路 ID 列表
    :return: 匹配的道路信息列表
    """
    return [road for road in all_roads if road['r_id'] in f_r_ids]

def find_roads_dict_by_ids(all_roads, f_r_ids):
    """
    从 all_roads 中查找 f_r_ids 对应的道路信息

    :param all_roads: 所有道路信息列表
    :param f_r_ids: 需要查找的道路 ID 列表
    :return: 匹配的道路信息列表
    """
    return [road for road in all_roads if road['r_id'] in f_r_ids]

# 示例用法
# f_r_ids = [1, 2, 3]  # 假设这是你要查找的 f_r_ids
# matching_roads = find_roads_by_ids(all_roads, f_r_ids)
# print(matching_roads)
def up_data(dataptath):
    db_file_list = []
    get_all_files(dataptath,db_file_list,".sq3")
    
    # 读取要保留的
    for file_path in db_file_list:
        filename, ext = os.path.splitext(os.path.basename(file_path))
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        # 使用自定义函数进行替换
        cursor.execute(f"delete from SDRoad;")
       
        conn.commit()
        conn.execute(f"VACUUM;")
        conn.close()


if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--datapath",type=str,required=True)
    args = parser.parse_args()
    up_data(args.datapath)

# python del_sd_road_data.py --datapath=C:\Users\13587\Downloads\CR-0424