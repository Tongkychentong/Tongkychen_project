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
import shutil
import time 
import requests 
import sqlite3
import schedule 
import logging
import psutil
from ks3.connection import Connection
import subprocess
import pandas as pd
from datetime import datetime, timedelta


c = Connection('AKLTqnHXMIRBR02DyEuVLFJM', 'ODXaZqLLNt9UAX1LVavU6lHtnzSiQHtcpmWxGX35', host='ks3-cn-beijing.ksyuncs.com')

# ks3://hdmap-process-public/mapapi-pb/test/pathinfodefectinfo/99_24_12_04_02_xformat_202412041730.zip


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

curtime = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
curtime = curtime.replace(' ', '__')
curtime = curtime.replace(':', '-')

if getattr(os.sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(os.sys.executable))
elif __file__:
    absPath = os.path.dirname(os.path.abspath(__file__))

dataPath = os.path.join(absPath, 'data')

if os.path.exists(dataPath) == False:
    os.mkdir(dataPath)

#拷贝目录【类似unix下的cp -r aa bb】
def copyDir(srcDir,dstDir):
    if os.path.exists(srcDir):
        __copyDir(srcDir,dstDir)
    else:
        print(srcDir+' not exist')
 
def __copyDir(srcDir,dstDir):
    if not os.path.exists(dstDir):
        shutil.copytree(srcDir,dstDir)
        return
    lists=os.listdir(srcDir)
    for lt in lists:
        srcPath=os.path.join(srcDir,lt)
        goalPath=os.path.join(dstDir,lt)
        if os.path.isfile(srcPath):
            shutil.copyfile(srcPath,goalPath)
        else:
            __copyDir(srcPath,goalPath)
              
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
                # print(file)
                file_list.append(file)
                

# 居中工具
center_tool_path = "/home/work/data/compilerwork/tool/new_center_line_241118_add"
# 删除白名单
route_del_tool_path = "/home/root123/data/zhangyifei/xformat_route_batcher"
turn_flag_tool_path = "/home/root123/xtwork/tool/experience_up/turn_flag_v2"
guidance_tool_pkg_path = "/home/root123/tool/guidance_tool_pkg"
versionwork_path = "/data/xformat/gxh/versionwork"
api = "http://10.235.108.163:7001"
v7_tool_path = "/home/python_project/hdmap-material-worker/etl-worker/bin/20250116"
ks3_tool = "/home/root123/xtwork/gxh"
v7_path = os.path.join(versionwork_path,"v7")
if os.path.isdir(v7_path) == False:
    os.mkdir(v7_path)
# if True:
#     center_tool_path = "/home/root123/tool/new_center_line_241118_add"
#     versionwork_path = "/data/xformat/gxh/data"
#     # api = "http://10.235.120.168:7001"
#     api = "http://10.235.108.163:7001"

def createUrl(KeyName,bucket):
    print(KeyName,bucket)
    # 填写Object完整路径。Object完整路径中不能包含Bucket名称。
    b = c.get_bucket(bucket)
    k = b.get_key(KeyName)
    if k:
        # 下载外链地址60s后过期
        # image_attrs为图片的指令或者样式参数字符串
        url = k.generate_url(35791393 * 60)
        # print(url)
        return url.replace("http:","https:")
    
def getDirByOne(dirpath):
    info = os.listdir(dirpath)
    nameByOne = info[0]
    if nameByOne is None:
        return False
    onePath = os.path.join(dirpath,nameByOne)
    return onePath

def sendMsg(content,href,title =""):
    s = json.dumps({
        "msg_type": "post", "content": {
            "post": {
                "zh_cn": {
                    "title": title, # ,
                    "content": [
                        [{
                            "tag": "at",
                            "user_id": "all"
                        },{
                            "tag": "a",
                            "text": content,
                            "href": href
                        }]
                    ]
                }
            }
        }
    })
    # {
    #               "tag": "a",
    #               "text": `http://10.235.108.163:7001${outPath}`,
    #               "href": `http://10.235.108.163:7001${outPath}`
    #             }
    openapitest = "https://open.f.mioffice.cn/open-apis/bot/v2/hook/6eda0ebb-f742-4951-952f-eeb189962638"
    openapi = "https://open.f.mioffice.cn/open-apis/bot/v2/hook/2cddfd1b-258d-4fdb-bdb0-e02e98ff8aa8"
    response = requests.post(openapi,data=s)
    response.close()
    

def process():
    # 获取当前日期
    current_date = datetime.now()
    # 计算日期
    zero_days_ago = current_date - timedelta(days=0)
    three_days_ago = current_date - timedelta(days=2)
    # 打印三天前的日期
    startTime = zero_days_ago.strftime('%Y-%m-%d')+" 00:00:00"
    threeTime = three_days_ago.strftime('%Y-%m-%d')+" 00:00:00"
    endTime = current_date.strftime('%Y-%m-%d')+" 23:59:59"
    out_path = do7vTool(startTime,endTime)
    out_name = os.path.basename(out_path)
    
    zip_cmd = f"cd {v7_path} && zip -r {out_name}.zip {out_name}/ -1"
    process = subprocess.run(zip_cmd, shell=True, text=True, capture_output=True)
    if process.stderr:
        print("错误信息:")
        sendvalue = f"[{out_name}]压缩异常 错误信息: \n {process.stderr} \n cmd \n {zip_cmd}"
        print(sendvalue)
        return
    ks3_pure = f"cd {ks3_tool} && ./ks3util-linux-amd64 cp {out_path}.zip ks3://hdmap-process-public/mapapi-pb/test/pathinfodefectinfo/"
    process = subprocess.run(ks3_pure, shell=True, text=True, capture_output=True)
    if process.stderr:
        print("错误信息:")
        sendvalue = f"[{out_name}]ks3异常 错误信息: \n {process.stderr} \n cmd \n {ks3_pure}"
        print(sendvalue)
        return
    pure_zip_ks3_address = f"mapapi-pb/test/pathinfodefectinfo/{out_name}.zip"
    print('统计数据',pure_zip_ks3_address)
    url = createUrl(pure_zip_ks3_address,"hdmap-process-public")
    print('统计数据url',url)
    sendMsg(f'{out_name}统计文件',url,f'{startTime} {endTime}统计工具执行结果')
    
    outpath =  do7vTool2(three_days_ago,current_date)
    out1 = f"{outpath}/{time.strftime('%Y-%m-%d%H:%M:%S', time.localtime())}.sq3"
    out2name = f"{time.strftime('%Y-%m-%d%H:%M:%S', time.localtime())}.xlsx"
    out2 = f"{outpath}/{out2name}"
    getVV(f"{outpath}/abnormal_issues_file.csv",out1,out2)
    ks3_pure = f"cd {ks3_tool} && ./ks3util-linux-amd64 cp {out2} ks3://hdmap-process-public/mapapi-pb/test/pathinfodefectinfo/"
    process = subprocess.run(ks3_pure, shell=True, text=True, capture_output=True)
    if process.stderr:
        print("错误信息:")
        sendvalue = f"[{out2}]ks3异常 错误信息: \n {process.stderr} \n cmd \n {ks3_pure}"
        print(sendvalue)
        return
    pure_zip_ks3_address = f"mapapi-pb/test/pathinfodefectinfo/{out2name}"
    url = createUrl(pure_zip_ks3_address,"hdmap-process-public")
    sendMsg(f'{out2name}聚合文件',url,f'{threeTime} {endTime}聚合文件')

users = "[('侯梅', 'xt'),('李迪', 'xt'),('袁硕', 'xt'),('史浩荣', 'xt'),('辛忠霖', 'xt'),('徐小英', 'xt'),('张喆', 'xt'),('杨晶', 'xt'),('齐星旭', 'xt'),('张世宇', 'xt'),('冯佳伟', 'xt'),('魏润良', 'xt'),('邓旺', 'xt'),('黄嵩', 'xt'),('冯鹏', 'xt'),('邢银培', 'xt'),('史凯旋', 'xt'),('王金', 'xt'),('李沂峰', 'xt'),('贺月华', 'xt'),('王星', 'xt'),('谢中昌', 'xt'),('李峰', 'xt'),('班文哲', 'xt'),('刘建达', 'xt'),('王雪涵', 'xt'),('杨明辉', 'xt'),('王旭东', 'xt'),('赵文学', 'xt'),('侯英旭', 'xt'),('薛贵纯', 'xt'),('李明爽', 'xt'),('郭丽莎', 'xt'),('郭宏伟', 'xt'),('李颖', 'xt'),('顾城垣', 'xt'),('张晓静', 'xt'),('刘文秀', 'xt'),('姚志飞', 'xt'),('付志洋', 'xt'),('王亚萍', 'xt'),('聂登朝', 'xt'),('秦飞', 'xt'),('张博', 'xt'),('丁浩笛', 'xt'),('杨鑫', 'xt'),('韩梦娇', 'xt'),('郭蕊', 'xt'),('王紫梦', 'xt'),('吕相欣', 'xt'),('杨坤炎', 'xt'),('李文韬', 'xt'),('赵琳莎', 'xt'),('郭腾龙', 'xt'),('刘晓琳', 'xt'),('李洁', 'xt'),('姚蕊', 'xt'),('杜培林', 'xt'),('刘旭辉', 'xt'),('南杰', 'xt'),('冉涛', 'xt'),('任萌萌', 'xt'),('宗宇鑫', 'xt'),('于纪豪', 'xt'),('柳淼雨', 'xt'),('许伊健', 'xt'),('李惠柳', 'xt'),('郭瑞涛', 'xt'),('李运豪', 'xt'),('胡春月', 'xt'),('张硕', 'xt'),('高俊丽', 'xt'),('祁丰裕', 'xt'),('梁天乐', 'xt'),('王振宇', 'xt'),('来志成', 'xt'),('王志豪', 'xt'),('李鑫雨', 'xt'),('刘锴贤', 'xt'),('李垚林', 'xt'),('葛锋辉', 'xt'),('李茂兴', 'xt'),('李涛', 'xt'),('任劲松', 'xt'),('乔昊天', 'xt'),('贾项博', 'xt'),('孟令恩', 'xt'),('李明岩', 'xt'),('冯惠', 'xt'),('柏兆烁', 'xt'),('王永康', 'xt'),('赵晓博', 'xt'),('马雨蝶', 'xt'),('徐明珠', 'xt'),('任自飞', 'xt'),('高子爽', 'xt'),('康硕', 'xt'),('岳泽霖', 'xt'),('周怡心', 'xt'),('武佳慧', 'xt'),('赵鹏宇', 'xt'),('高鹏', 'xt'),('张家宣', 'xt'),('陈颖', 'xt'),('陈培旭', 'xt'),('韩凤', 'xt'),('罗旭', 'xt'),('耿宁', 'xt'),('王宏宇', 'xt'),('郝云伟', 'xt'),('杨轶', 'xt'),('孟万雨', 'xt'),('戈红义', 'xt'),('耿佳欣', 'xt'),('李若泉', 'xt'),('杨翔宇', 'xt'),('孙旭', 'xt'),('张义德', 'xt'),('董钊宇', 'xt'),('洪万煜', 'xt'),('王仕鹏', 'xt'),('刘力萌', 'xt'),('吴俊洁', 'xt'),('李浩雨', 'xt'),('陈梓健', 'xt'),('王志远', 'xt'),('王士林', 'xt'),('张宇琛', 'xt'),('边宇轩', 'xt'),('张佳拓', 'xt'),('袁锦荣', 'xt'),('田墨晗', 'xt'),('郭宇轩', 'xt'),('柳宣利', 'xt'),('李文辉', 'xt'),('张浩辉', 'xt'),('崔华宇', 'xt'),('张倩倩', 'xt'),('孙美茹', 'xt'),('郑新颖', 'xt'),('吴炫樟', 'xt'),('乔新禹', 'xt'),('张焱彪', 'xt'),('秦浩翔', 'xt'),('刘婷', 'xt'),('赵万阳', 'xt'),('王健鹏', 'xt'),('马连杰', 'xt'),('孙志浩', 'xt'),('夏悦', 'xt'),('李雨', 'xt'),('卫芊芊', 'xt'),('张浩楠', 'xt'),('贾琳', 'xt'),('张萌', 'xt'),('王依萍', 'xt'),('李彦学', 'xt'),('周海龙', 'xt'),('闫春霞', 'xt'),('刘骁涵', 'xt'),('张春雨', 'xt'),('陈学佳', 'xt'),('柴姝祺', 'xt'),('纪芸倩', 'xt'),('李佳博', 'xt'),('赵丽娜', 'xt'),('李佳', 'xt'),('陈晓南', 'xt'),('王晨伟', 'xt'),('王文倩', 'xt'),('张博冉', 'xt'),('周梦欣', 'xt'),('史洋潮', 'xt'),('毛佳凝', 'xt'),('薛新杰', 'xt'),('韩佳怡', 'xt'),('蒋思语', 'xt'),('安东星', 'xt'),('吴瑞', 'xt'),('赵思诚', 'xt'),('杨可欣', 'xt'),('张林荟', 'xt'),('施云凤', 'xt'),('刘雨鑫', 'xt'),('邹婉晴', 'xt'),('姚涵春', 'xt'),('伍雨婷', 'xt'),('崔亚强', 'xt'),('张宇乐', 'xt'),('武宇萌', 'xt'),('姚佳欣', 'xt'),('任宇新', 'xt'),('贾志祥', 'xt'),('张耀华', 'xt'),('李润泽', 'xt'),('赵峻霄', 'xt'),('段艳林', 'xt'),('吕朝阳', 'xt'),('鲁新建', 'xt'),('王炎博', 'xt'),('王成基', 'xt'),('谢宏宇', 'xt'),('王蝶蝶', 'xt'),('乔敏', 'xt'),('刘萍萍', 'xt'),('白茹雪', 'xt'),('王维佳', 'xt'),('邹雪莹', 'xt'),('刘兵', 'xt'),('刘峥', 'xt'),('侯力银', 'xt'),('张俊耀', 'xt'),('杨泽瑛', 'xt'),('石砚晖', 'xt'),('杨佳旭', 'xt'),('刘孟钊', 'xt'),('崔瀚文', 'xt'),('付塞拓', 'xt'),('史玉明', 'xt'),('殷欣乐', 'xt'),('吴雨峰 ', 'xt'),('付天明', 'xt'),('郭雪楠', 'xt'),('殷学智', 'xt'),('白鹤', 'xt'),('杨浩', 'xt'),('李凉', 'xt'),('郭泽文', 'xt'),('刘佳康', 'xt'),('郭瑞珍', 'xt'),('张梦琦', 'xt'),('王海涛', 'xt'),('杨江', 'xt'),('张雅琦', 'xt'),('张博远', 'xt'),('李志豪', 'xt'),('王晓峰', 'xt'),('陈奇', 'xt'),('张爱媛', 'xt'),('张怡飞', 'xt'),('刘敏娜', 'xt'),('于伯晟', 'xt'),('郭利飞', 'xt'),('周梦梦', 'xt'),('张永庚', 'xt'),('张豪毅', 'xt'),('张午春', 'xt'),('赵慧敏', 'xt'),('孙肖云', 'xt'),('丁琳澍', 'xt'),('王亚洲', 'xt'),('韩松旭', 'xt'),('杨梦飞', 'xt'),('郝飞虎', 'xt'),('高煜东', 'xt'),('于淼', 'xt'),('张达', 'xt'),('杨奥', 'xt'),('边蕊', 'xt'),('李雅楠', 'xt'),('李阳', 'xt'),('李博', 'xt'),('王宇彤', 'xt'),('翟瑞雨', 'xt'),('宋洪凯', 'xt'),('付钰琪', 'xt'),('张明珠', 'xt'),('康鑫', 'xt'),('王起', 'xt'),('王新波', 'xt'),('赵婉琦', 'xt'),('马玉岩', 'xt'),('单一文', 'xt'),('靳峰', 'xt'),('任磊', 'xt'),('韩双凤', 'xt'),('赵成功', 'xt'),('赵成龙', 'xt'),('毕建宁', 'xt'),('焦宇航', 'xt'),('赵鹏飞', 'xt'),('赵嘉欣', 'xt'),('薛佳明', 'xt'),('郭晓辉', 'xt'),('王文禄', 'xt'),('王祎航', 'xt'),('王鑫宇', 'xt'),('韩欣悦', 'xt'),('尹汉一', 'xt'),('李振辅', 'xt'),('韩荣誉', 'xt'),('李晓庆', 'xt'),('杜文豪', 'xt'),('王胜慧', 'xt'),('蒋焕涛', 'xt'),('李浩然', 'xt'),('张浩然', 'xt'),('陈凯', 'xt'),('蔡依涵', 'xt'),('杨茜', 'xt'),('彭琳娜', 'xt'),('李文倩', 'xt'),('张雨壕', 'xt'),('李浩亮', 'xt'),('马健康', 'xt'),('黄胜飞', 'xt'),('许晓雪', 'xt'),('王祎', 'xt'),('昝放', 'xt'),('杨焘硕', 'xt'),('陈正杰', 'xt'),('吴子旭', 'xt'),('卫子兵', 'xt'),('何天泽', 'xt'),('张信哲', 'xt'),('张小雨', 'xt'),('丁家乐', 'xt'),('李文静', 'xt'),('卫自立', 'xt'),('杨晨', 'xt'),('王腾甲', 'xt'),('刘祖辉', 'xt'),('刘孝', 'xt'),('郭蕊1', 'xt'),('宋宏扬', 'xt'),('石宏浩', 'xt'),('梁晓雯', 'xt'),('化一阔', 'xt'),('徐永智', 'xt'),('温佳璇', 'kd'),('王玉娇', 'kd'),('张弛', 'kd'),('赵福有', 'kd'),('林启涵', 'kd'),('周芷茵', 'kd'),('白明月', 'kd'),('冯安琦', 'kd'),('周乐垚', 'kd'),('王歆雅', 'kd'),('王佳伟', 'kd'),('唐红红', 'kd'),('陈洁非', 'kd'),('杨文艺', 'kd'),('句静静', 'kd'),('冯志强', 'kd'),('赖欢', 'kd'),('刘磊', 'kd'),('苏佛佛', 'kd'),('张婧姝', 'kd'),('韩洋', 'kd'),('周琪', 'kd'),('杨力嘉', 'kd'),('汪晓晨', 'kd'),('张泽淇', 'kd'),('康丹丹', 'kd'),('贺龙坤', 'kd'),('段悠然', 'kd'),('欧浩乾', 'kd'),('刘旭', 'kd'),('张文武', 'kd'),('张永斗', 'kd'),('王正虎', 'kd'),('张鑫鑫', 'kd'),('高杰', 'kd'),('刘海宇', 'kd'),('张越', 'kd'),('段成超', 'kd'),('李明慧', 'kd'),('王昊', 'kd'),('何慎冉', 'kd'),('王世建', 'kd'),('赵翠玲', 'kd'),('张聪', 'kd'),('陈禹存', 'kd'),('郭垚', 'kd'),('赵一杰', 'kd'),('何鑫鑫', 'kd'),('张秋实', 'kd'),('路博菲', 'kd'),('冯佳鑫', 'kd'),('杨宇航', 'kd'),('李子贞', 'kd'),('祁潇', 'kd'),('敖宇松', 'kd'),('焦东芳', 'kd'),('付浩杰', 'kd'),('王玉杰', 'kd'),('王壮', 'kd'),('杨谢蓝', 'kd'),('刘新蕊', 'kd'),('刘才华', 'kd'),('胡广杰', 'kd'),('刘继鑫', 'kd'),('陈洁菲', 'kd'),('张驰', 'kd')]"

def do7vTool(startTime,endTime):
    print(startTime,endTime)
    out_path = os.path.join(v7_path,time.strftime('%Y-%m-%d-%H%M', time.localtime())) 
    if os.path.isdir(out_path) == True:
        shutil.rmtree(out_path)
    os.mkdir(out_path)
    timewait = 60
    cmd = f'cd {v7_tool_path} && python3 request_normal_isseu_mutli.py "{startTime}" "{endTime}" "{timewait}" "{users}" "{out_path}"'
    process = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    print("执行信息:---",cmd)
    print(process.stdout)
    print(process.stderr)
    return out_path

def do7vTool2(startTime,endTime):
    print(startTime,endTime)
    # 定义起始和结束日期
    start_date = startTime  # 示例起始日期：2023年1月1日
    end_date = endTime    # 示例结束日期：2023年1月5日

    # 计算两个日期之间的天数
    delta = end_date - start_date

    # 遍历每一天
    times = []
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        times.append((f"{day.strftime('%Y-%m-%d')} 00:00:00"))
    out_path = os.path.join(v7_path,time.strftime('%Y-%m-%d-%H%M', time.localtime())) 
    if os.path.isdir(out_path) == True:
        shutil.rmtree(out_path)
    os.mkdir(out_path)
    timewait = 60
    cmd = f'python3 /home/python_project/hdmap-material-worker/etl-worker/bin/20250123/request_normal_isseu_mutli.py "{str(times)}" "" "120" "{users}" "{out_path}"'
    process = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    print("执行信息:---",cmd)
    print(process.stdout)
    print(process.stderr)
    return out_path


def get_all_files(file_path,file_list,file_filter=".sq3"):
    for file_name in os.listdir(file_path):
        file = os.path.join(file_path,file_name)
        if os.path.isdir(file):
            get_all_files(file,file_list,file_filter)
        else:
            if os.path.basename(file).endswith(file_filter) :
                # print(file)
                file_list.append(file)
                
def get_file_list(directory):
    file_list = os.listdir(directory)
    return file_list   

def diskSpaceCheck(dir):
    info = os.statvfs(dir)
    free_size = info.f_bsize * info.f_bavail / 1024 / 1024 / 1024
    # print(f'{dir}可用磁盘空间:{round(free_size, 3)}G')
    return round(free_size, 3)

def neicunCheck():
    available_size = psutil.virtual_memory().available / 1024 /1024 / 1024
    return round(available_size, 3)

datapath = '/home/root123/xtwork/base_xformat'

INFO = '\
CREATE TABLE [INFO](\
"TASK_ID" TEXT,\
"TASK_STEP" TEXT,\
"WORK_STATUS" TEXT,\
"END_TIME" TEXT,\
"TRACK_NUM" TEXT,\
"USER" TEXT,\
"TIMES" TEXT\
);\
'
# 结束时间
def getVV(PATCH,outpath,outfile):
    roadIdData = pd.read_csv(PATCH)
    connOut = sqlite3.connect(outpath)
    cursorOut=connOut.cursor()
    cursorOut.execute(INFO)
    for index,row in roadIdData.iterrows():
        sql = f"INSERT INTO INFO (TASK_ID,TASK_STEP,WORK_STATUS,END_TIME,TRACK_NUM,USER,TIMES) VALUES ( '{row['主任务ID']}','{row['流程节点名称']}','{row['作业状态']}','{row['结束时间']}','{row['可用轨迹数']}','{row['分配人员']}','{row['编辑用时']}')"
        # print(sql)
        cursorOut.execute(sql)
    connOut.commit()
    sql3 = "select TASK_ID,USER from INFO where TASK_STEP in ('重定位作业')"
    cursorOut.execute(sql3)
    workdata = cursorOut.fetchall()
    workdict = {}
    for row in workdata:
        workdict[str(row[0])] = row[1]
        
    sql2 = "select TASK_ID,TASK_STEP,USER,sum(TIMES),max(END_TIME),TRACK_NUM from INFO where WORK_STATUS in ('任务作业','任务质检','1次返修','1次核标') group by TASK_ID,TASK_STEP,USER"
    cursorOut.execute(sql2)
    data = cursorOut.fetchall()
    testdata = []
    makedata = []
    for row in data:
        if row[1] == '重定位质检':
            make_user = ""
            if row[0] in workdict.keys():
                make_user = workdict[row[0]]
            testdata.append({
                '主任务ID': row[0],
                '流程节点名称': row[1],
                '制作人员': make_user,
                '分配人员': row[2],
                '编辑用时': row[3],
                '结束时间': row[4],
                '可用轨迹数': row[5],
            })
        else:
            makedata.append({
                '主任务ID': row[0],
                '流程节点名称': row[1],
                '分配人员': row[2],
                '编辑用时': row[3],
                '结束时间': row[4],
                '可用轨迹数': row[5],
            })
            # print(row[0],row[1],row[2])
    df1 = pd.DataFrame(makedata)        
    df2 = pd.DataFrame(testdata)        
    with pd.ExcelWriter(outfile) as writer:
        # 将df1写入名为'Sheet1'的工作表
        df1.to_excel(writer, sheet_name='重定位制作', index=False)
        # 将df2写入名为'Sheet2'的工作表
        df2.to_excel(writer, sheet_name='重定位质检', index=False)        
    connOut.close()
# schedule.every().day.at("23:59").do(process)
# while True:  
#     schedule.run_pending()  
#     time.sleep(1)  
if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    process()
   
    
    # out1 = f"/data/xformat/gxh/versionwork/{time.strftime('%Y-%m-%d%H:%M:%S', time.localtime())}.sq3"
    # out2 = f"/data/xformat/gxh/versionwork/{time.strftime('%Y-%m-%d%H:%M:%S', time.localtime())}.xlsx"
    # getVV("/data/xformat/gxh/versionwork/abnormal_issues_file.csv",out1,out2)
    

# python3 jyVersionCheck.py --datapath=/data/xformat/gxh/versionwork/version/99_25_01_22_01_xformat_202501231340 --outpath=/data/xformat/gxh/outtt