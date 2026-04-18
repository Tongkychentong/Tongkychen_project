import sqlite3
import os
import argparse

if getattr(os.sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(os.sys.executable))
elif __file__:
    absPath = os.path.dirname(os.path.abspath(__file__))
allPagesPath = os.path.join(absPath, "all\\")

create_table_sql = '\
CREATE TABLE "CSH" (\
    "CSH_ID" BIGINT,\
    "TILE" INT,\
    "R_ID" BIGINT,\
    "L_ID" BIGINT,\
    "POINT_ID" INT,\
    "CURVATURE" FLOAT,\
    "SLOPE" FLOAT,\
    "XSLOPE" FLOAT,\
    "HEADING" FLOAT\
);\
CREATE TABLE "DashPoint" (\
    "DASHPNT_ID" BIGINT,\
    "TILE" INT,\
    "LM_ID" TEXT NOT NULL,\
    "SEQ"   TEXT NOT NULL,\
    "Geometry" BLOB\
);\
CREATE TABLE DefectInfo (\
    DFT_ID BIGINT,\
    ISSUE_ID char(20), \
    TILE INTEGER,\
    STATUS INTEGER,\
    TIME char(160),\
    TYPE char(20),\
    LAYER INTEGER,\
    ID BIGINT,\
    SOFFSET INTEGER,\
    EOFFSET INTEGER,\
    CREATTIME char(20),\
    UPDATETIME char(20),\
    VD_VERSION char(20),\
    FM_VERSION char(20),\
    PB_TIME CHAR(20),\
    GEOMETRY BLOB\
);\
CREATE TABLE "JunctionArea" (\
    "JA_ID" BIGINT,\
    "TILE" INT,\
    "TYPE" INT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
CREATE TABLE "LaneSplitMerge" (\
    "LSM_ID" BIGINT,\
    "TILE" INT,\
    "L_ID" BIGINT,\
    "TYPE" INT,\
    "SOFFSET" INT,\
    "EOFFSET" INT\
);\
CREATE TABLE PatchInfo (\
    PCINF_ID BIGINT,\
    ISSUE_ID char(20), \
    NODD_ID BIGINT,\
    NODD_TYPE INTEGER,\
    TILE INTEGER,\
    ISSUE_TYPE char(20),\
    PCINF_TYPE INTEGER,\
    LAYER INTEGER,\
    FEATURE_ID Bigint,\
    FIELD char(10),\
    OLD_VALUE char(100),\
    NEW_VALUE char(20),\
    SOFFSET INTEGER,\
    EOFFSET INTEGER,\
    CREATTIME char(20),\
    UPDATETIME CHAR(20),\
    VD_VERSION char(20),\
    FM_VERSION char(20),\
    PB_TIME char(20),\
    GEOMETRY BLOB\
);\
CREATE TABLE "PolygonalFacility" (\
    "PF_ID" BIGINT,\
    "TILE" INT,\
    "PF_TYPE" INT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
CREATE TABLE "Lane" (\
    "L_ID" BIGINT,\
    "TILE" INT,\
    "R_ID" BIGINT,\
    "L_LM_ID" TEXT NULL,\
    "L_LM_DIR" TEXT NULL,\
    "R_LM_ID" TEXT NULL,\
    "R_LM_DIR" TEXT NULL,\
    "LG_ID" BIGINT,\
    "L_TYPE" BIGINT,\
    "L_NO" INT,\
    "DIRECTION" INT,\
    "IS_VIRTUAL" INT,\
    "JA_ID" TEXT NULL,\
    "T_DRCT" INT,\
    "LENGTH" INT,\
    "TRFC_FLG" INT,\
    "F_TILE" TEXT NULL,\
    "F_L_ID" TEXT NULL,\
    "T_TILE" TEXT NULL,\
    "T_L_ID" TEXT NULL,\
    "Geometry" BLOB\
);\
CREATE TABLE "LaneMarking" (\
    "LM_ID" BIGINT,\
    "TILE" INT,\
    "R_ID" TEXT,\
    "TYPE" TEXT,\
    "COLOR" INT,\
    "WIDTH" INT,\
    "LENGTH" INT,\
    "LM_DRCT" INT,\
    "LDM_TYPE" INT,\
    "LM_NO" TEXT,\
    "LG_ID" TEXT,\
    "REAL_LM_ID" TEXT NULL,\
    "CONFIDENCE" INT,\
    "Geometry" BLOB\
);\
CREATE TABLE "LaneTypeInfo" (\
    "LTI_ID" BIGINT,\
    "TILE" INT,\
    "L_ID" BIGINT,\
    "L_TYPE" BIGINT,\
    "LA_ID" BIGINT,\
    "SOFFSET" INT,\
    "EOFFSET" INT\
);\
CREATE TABLE "LaneWidthInfo" (\
    "LWI_ID" BIGINT,\
    "TILE" INT,\
    "L_ID" BIGINT,\
    "WIDTH" INT,\
    "SOFFSET" INT,\
    "EOFFSET" INT\
);\
CREATE TABLE "Pole" (\
    "POLE_ID" BIGINT,\
    "TILE" INT,\
    "POLE_TYPE" INT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
CREATE TABLE "RestrictionLine" (\
    "RES_LN_ID" BIGINT,\
    "TILE" INT,\
    "R_ID" BIGINT,\
    "L_ID" BIGINT,\
    "RESLN_TYPE" INT,\
    "SRC_LAYER" INT,\
    "LAYER_TILE" INT,\
    "LAYER_ID" TEXT NULL,\
    "MAX_SPEED" INT,\
    "MIN_SPEED" INT,\
    "START_DAY" TEXT NULL,\
    "END_DAY" TEXT NULL,\
    "CUR_DAY" INT,\
    "START_TIME" TEXT NULL,\
    "END_TIME" TEXT NULL,\
    "CUR_TIME" INT,\
    "WEEK_TYPE" INT,\
    "CUR_WEEK" INT,\
    "VEHICLE" INT,\
    "CURVEHICLE" INT,\
    "WEATHER" INT,\
    "CURWEATHER" INT,\
    "SEASON" INT,\
    "CUR_SEASON" INT,\
    "HOLIDAY" INT,\
    "CURHOLIDAY" INT,\
    "HOVNUM" INT,\
    "CUR_HOVNUM" INT,\
    "LPCODE" INT,\
    "CUR_LPCODE" INT,\
    "SOFFSET" INT,\
    "EOFFSET" INT\
);\
CREATE TABLE "Road" (\
    "R_ID" BIGINT,\
    "TILE" INT,\
    "NAME_CN" TEXT DEFAULT "",\
    "ROUTE_NO" TEXT DEFAULT "",\
    "NAME_EN" TEXT DEFAULT "",\
    "NAME_PY" TEXT DEFAULT "",\
    "NAME_ALIAS" TEXT DEFAULT "",\
    "NAME_EX" TEXT DEFAULT "",\
    "RC" INT,\
    "FORM_WAY" INT,\
    "FC" INT,\
    "R_TYPE" INT,\
    "NUM_LANE" INT,\
    "DVB_LANES" INT,\
    "LENGTH" INT,\
    "TRFC_FLG" INT,\
    "DIRECTION" INT,\
    "F_TILE" TEXT NULL,\
    "F_R_ID" TEXT NULL,\
    "F_RN_ID" INT,\
    "T_TILE" TEXT NULL,\
    "T_R_ID" TEXT NULL,\
    "T_RN_ID" INT,\
    "CONNECTOR" INT,\
    "LG_ID" BIGINT,\
    "WIDTH" INT,\
    "L_LM_ID" TEXT NULL,\
    "L_LM_DIR" TEXT NULL,\
    "R_LM_ID" TEXT NULL,\
    "R_LM_DIR" TEXT NULL,\
    "REL_OBJS" TEXT NULL,\
    "ADMINCODE" TEXT NULL,\
    "Geometry" BLOB\
);\
CREATE TABLE "RoadMark" (\
    "RM_ID" BIGINT,\
    "TILE" INT,\
    "RM_TYPE" TEXT NULL,\
    "MAX_SPEED" TEXT,\
    "MIN_SPEED" TEXT,\
    "SPLM_GRPID" BIGINT,\
    "START_DAY" TEXT NULL,\
    "END_DAY" TEXT NULL,\
    "CUR_DAY" INT,\
    "START_TIME" TEXT NULL,\
    "END_TIME" TEXT NULL,\
    "CUR_TIME" INT,\
    "WEEK_TYPE" INT,\
    "CUR_WEEK" INT,\
    "VEHICLE" INT,\
    "CURVEHICLE" INT,\
    "WEATHER" INT,\
    "CURWEATHER" INT,\
    "SEASON" INT,\
    "CUR_SEASON" INT,\
    "HOLIDAY" INT,\
    "CURHOLIDAY" INT,\
    "HOVNUM" INT,\
    "CUR_HOVNUM" INT,\
    "LPCODE" INT,\
    "CUR_LPCODE" INT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
CREATE TABLE "TrafficSign" (\
    "TS_ID" BIGINT,\
    "TILE" INT,\
    "TS_TYPE" INT,\
    "TS_SUBTYPE" INT,\
    "SUBTS_FLAG" INT,\
    "MAX_SPEED" TEXT NULL,\
    "MIN_SPEED" TEXT NULL,\
    "START_DAY" TEXT NULL,\
    "END_DAY" TEXT NULL,\
    "CUR_DAY" INT,\
    "START_TIME" TEXT NULL,\
    "END_TIME" TEXT NULL,\
    "CUR_TIME" INT,\
    "WEEK_TYPE" INT,\
    "CUR_WEEK" INT,\
    "VEHICLE" INT,\
    "CURVEHICLE" INT,\
    "WEATHER" INT,\
    "CURWEATHER" INT,\
    "SEASON" INT,\
    "CUR_SEASON" INT,\
    "HOLIDAY" INT,\
    "CURHOLIDAY" INT,\
    "HOVNUM" INT,\
    "CUR_HOVNUM" INT,\
    "LPCODE" INT,\
    "CUR_LPCODE" INT,\
    "YAW" DOUBLE,\
    "SHAPE" INT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
CREATE TABLE "StopLine" (\
    "SL_ID" BIGINT,\
    "TILE" INT,\
    "SL_TYPE" INT,\
    "TL_FLAG" INT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
CREATE TABLE "TrafficLight" (\
    "TL_ID" BIGINT,\
    "TILE" INT,\
    "TL_TYPE" INT,\
    "PHASE" INT,\
    "TLGRP_NUM" INT,\
    "DIRECTION" INT,\
    "NUM_BULBS" INT,\
    "HIGHT" INT,\
    "YAW" FLOAT,\
    "REL_L_TILE" TEXT NULL,\
    "REL_L_ID" TEXT NULL,\
    "REL_L_TYPE" TEXT NULL,\
    "REL_R_TILE" TEXT NULL,\
    "REL_R_ID" TEXT NULL,\
    "REL_R_TYPE" TEXT NULL,\
    "RELOBJTILE" TEXT NULL,\
    "REL_OBJ_ID" TEXT NULL,\
    "RELOBJTYPE" TEXT NULL,\
    "GEOMETRY" BLOB NULL\
);\
'
def gettableinfo(cur=sqlite3.Cursor):
    cur.execute("SELECT name from sqlite_master WHERE TYPE = 'table'")
    table_info = []
    for row in cur.fetchall():
        table_info.append(row[0])
    return table_info

def getallfiles(datapath,file_list=dict(),file_name=".sq3"):
   
    for file in os.listdir(datapath):
        file_path = os.path.join(datapath,file)
        if os.path.isdir(file_path):
            getallfiles(file_path,file_list,file_name)
        else:
            file_base_name = os.path.basename(file)
            if str(file_base_name.find(file_name)) != -1:
                file_list[file_base_name[:-4]] = file_path
def get_all_files(file_path,file_list,file_filter=".sq3"):
    for file_name in os.listdir(file_path):
        file = os.path.join(file_path,file_name)
        if os.path.isdir(file):
            get_all_files(file,file_list,file_filter)
        else:
            if os.path.basename(file).endswith(file_filter) :
                # print(file)
                file_list.append(file)
     
def modify_tl_yaw(old_value):
    return round(old_value,4)
def idsCODES(data,sp = ',',i = 0):
    REST = ""
    RESTINDEX = -1
    for y in data:
        if y[i] is None:
          continue
        RESTINDEX += 1
        if RESTINDEX != 0:
            REST = REST + sp
        REST = REST + str(y[i])
    # print(REST)
    return REST

def cm_to_km_rounded(cm):  
    km = cm / 100000  
    return round(km, 4)  

def process(dataptath=str):
    UR_Code = os.path.join(absPath, 'UR_Code_polygon_new.sq3')
    # connUR_Code = sqlite3.connect(UR_Code)
    # cursorUR_Code=connUR_Code.cursor()
    
    # print(dataptath)
    db_file_list = []
    get_all_files(dataptath,db_file_list,".sq3")
    for file_path in db_file_list:
        # print("11Process:", file_path)
        filename, ext = os.path.splitext(os.path.basename(file_path))
        # cursorUR_Code.execute(f'select City_Code,TileId from ur_code_polygon where TileId = {filename}')
        # codedata = cursorUR_Code.fetchall()
        code = 0
        # if len(codedata) != 0:
        #     code = codedata[0][0]
            
        conn = sqlite3.connect(file_path)
        cur = conn.cursor()
        # sqlpatch = "delete from PatchInfo where UPDATETIME < '2023-08-01 00:00:00'"
        # cur.execute(sqlpatch)
        # sqlpatch2 = "delete from PatchInfo where ISSUE_ID in ('SpeedLimit-Batch','LargeAngle-Batch')"
        # cur.execute(sqlpatch2)
        sqltrfc = "delete from PatchInfo where FIELD = 'trfc_flg'"
        cur.execute(sqltrfc)
        
        # 查询车道补丁
        # sql_lane = 'select TILE,SUM(LENGTH) from (SELECT B.TILE,B.R_ID,B.L_ID,A.FEATURE_ID,B.LENGTH FROM PatchInfo A LEFT JOIN Lane B ON A.FEATURE_ID = B.L_ID WHERE A.LAYER = 2 GROUP BY B.R_ID) GROUP BY TILE'
        query_lane = "SELECT B1.R_ID FROM PatchInfo A1 LEFT JOIN Lane B1 ON A1.FEATURE_ID = B1.L_ID WHERE A1.LAYER = 2 GROUP BY B1.R_ID"
        cur.execute(query_lane)
        laneIds = idsCODES(cur.fetchall())
        # tile = 'GROUP BY TILE'
        tile = ''
        sql_lane = f'select TILE,SUM(LENGTH) from Road {tile} where R_ID in ({laneIds}) {tile}'
        # 查询道路补丁
        query_road = f"SELECT R.R_ID FROM PatchInfo A LEFT JOIN Road R ON A.FEATURE_ID = R.R_ID WHERE A.LAYER = 1 and R.R_ID not in ({laneIds}) GROUP BY R.R_ID"
        cur.execute(query_road)
        roadIds = idsCODES(cur.fetchall())
        sql_road = f'select TILE,SUM(LENGTH) from Road where R_ID in ({roadIds}) {tile}'
        # 查询边线补丁  
        query_lm = f'SELECT B1.R_ID FROM PatchInfo A1 LEFT JOIN LaneMarking B1 ON A1.FEATURE_ID = B1.LM_ID WHERE A1.LAYER = 3 and B1.R_ID not in ({laneIds}) AND B1.R_ID not in ({laneIds}) GROUP BY B1.R_ID'
        cur.execute(query_lm)
        lmIds = idsCODES(cur.fetchall())
        sql_lm = f'select TILE,SUM(LENGTH) from Road where R_ID in ({lmIds}) {tile}'
        # 查询限速补丁  
        query_res = f'SELECT B1.R_ID FROM PatchInfo A1 LEFT JOIN RestrictionLine B1 ON A1.FEATURE_ID = B1.RES_LN_ID WHERE A1.LAYER = 14 and B1.R_ID not in ({laneIds}) AND B1.R_ID not in ({laneIds}) AND B1.R_ID not in ({lmIds}) GROUP BY B1.R_ID'
        cur.execute(query_res)
        resIds = idsCODES(cur.fetchall())
        sql_res = f'select TILE,SUM(LENGTH) from Road where R_ID in ({resIds}) {tile}'
        
        
        kml = 0
        cur.execute(sql_lane)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
        cur.execute(sql_road)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
        cur.execute(sql_lm)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
                
        cur.execute(sql_res)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
        defDif = processOneDef(cur)
        
        # 获取缺陷点数
        
        # 获取补丁点数
        sql_patchs =  'select TILE from PatchInfo'
        cur.execute(sql_patchs)
        defs = len(cur.fetchall())
        sql_issues = 'select ISSUE_ID from PatchInfo group by ISSUE_ID'
        cur.execute(sql_issues)
        issues = len(cur.fetchall())
        # if (kml != 0 or defDif['kml'] != 0):
        print(filename,code,'补丁',cm_to_km_rounded(kml),issues,defs,'缺陷',cm_to_km_rounded(defDif['kml']),defDif['issues'],defDif['defs'])
        
        # conn.commit()
        conn.close()

def processOneDef(cur):
    sqlT3 = "delete from DefectInfo where TYPE = 3"
    cur.execute(sqlT3)
    
    # 查询车道补丁
    # sql_lane = 'select TILE,SUM(LENGTH) from (SELECT B.TILE,B.R_ID,B.L_ID,A.FEATURE_ID,B.LENGTH FROM PatchInfo A LEFT JOIN Lane B ON A.FEATURE_ID = B.L_ID WHERE A.LAYER = 2 GROUP BY B.R_ID) GROUP BY TILE'
    query_lane = "SELECT B1.R_ID FROM DefectInfo A1 LEFT JOIN Lane B1 ON A1.ID = B1.L_ID WHERE A1.LAYER = 2 GROUP BY B1.R_ID"
    cur.execute(query_lane)
    laneIds = idsCODES(cur.fetchall())
    # tile = 'GROUP BY TILE'
    tile = ''
    sql_lane = f'select TILE,SUM(LENGTH) from Road {tile} where R_ID in ({laneIds}) {tile}'

    query_road = f"SELECT R.R_ID FROM DefectInfo A LEFT JOIN Road R ON A.ID = R.R_ID WHERE A.LAYER = 1 and R.R_ID not in ({laneIds}) GROUP BY R.R_ID"
    # print(query_road)
    cur.execute(query_road)
    roadIds = idsCODES(cur.fetchall())
    
    sql_road = f'select TILE,SUM(LENGTH) from Road where R_ID in ({roadIds}) {tile}'
    query_lm = f'SELECT B1.R_ID FROM DefectInfo A1 LEFT JOIN LaneMarking B1 ON A1.ID = B1.LM_ID WHERE A1.LAYER = 3 and B1.R_ID not in ({laneIds}) AND B1.R_ID not in ({laneIds}) GROUP BY B1.R_ID'
    # print(query_lm)
    cur.execute(query_lm)
    lmIds = idsCODES(cur.fetchall())
    
    sql_lm = f'select TILE,SUM(LENGTH) from Road where R_ID in ({lmIds}) {tile}'
    
    kml = 0
    cur.execute(sql_lane)
    data = cur.fetchall()
    for item in data:
        if item[0] is not None:
            kml += item[1]
    cur.execute(sql_road)
    data = cur.fetchall()
    for item in data:
        if item[0] is not None:
            kml += item[1]
    cur.execute(sql_lm)
    data = cur.fetchall()
    for item in data:
        if item[0] is not None:
            kml += item[1]
    # if (kml != 0):
        # print(filename,'缺陷',cm_to_km_rounded(kml))
    sql_defs = 'select TILE from DefectInfo'
    cur.execute(sql_defs)
    defs = len(cur.fetchall())
    sql_issues = 'select ISSUE_ID from DefectInfo group by ISSUE_ID'
    cur.execute(sql_issues)
    issues = len(cur.fetchall())
    return {
        'kml': kml,
        'defs': defs,
        'issues': issues
    }
        
def processDef(dataptath=str):
    # print(dataptath)
    db_file_list = []
    get_all_files(dataptath,db_file_list,".sq3")
    for file_path in db_file_list:
        # print("11Process:", file_path)
        filename, ext = os.path.splitext(os.path.basename(file_path))
        conn = sqlite3.connect(file_path)
        cur = conn.cursor()
        # sqlT3 = "delete from DefectInfo where TYPE = 3"
        # cur.execute(sqlT3)
        
        # 查询车道补丁
        # sql_lane = 'select TILE,SUM(LENGTH) from (SELECT B.TILE,B.R_ID,B.L_ID,A.FEATURE_ID,B.LENGTH FROM PatchInfo A LEFT JOIN Lane B ON A.FEATURE_ID = B.L_ID WHERE A.LAYER = 2 GROUP BY B.R_ID) GROUP BY TILE'
        query_lane = "SELECT B1.R_ID FROM DefectInfo A1 LEFT JOIN Lane B1 ON A1.ID = B1.L_ID WHERE A1.LAYER = 2 GROUP BY B1.R_ID"
        cur.execute(query_lane)
        laneIds = idsCODES(cur.fetchall())
        # tile = 'GROUP BY TILE'
        tile = ''
        sql_lane = f'select TILE,SUM(LENGTH) from Road {tile} where R_ID in ({laneIds}) {tile}'

        query_road = f"SELECT R.R_ID FROM DefectInfo A LEFT JOIN Road R ON A.ID = R.R_ID WHERE A.LAYER = 1 and R.R_ID not in ({laneIds}) GROUP BY R.R_ID"
        # print(query_road)
        cur.execute(query_road)
        roadIds = idsCODES(cur.fetchall())
        
        sql_road = f'select TILE,SUM(LENGTH) from Road where R_ID in ({roadIds}) {tile}'
        query_lm = f'SELECT B1.R_ID FROM DefectInfo A1 LEFT JOIN LaneMarking B1 ON A1.ID = B1.LM_ID WHERE A1.LAYER = 3 and B1.R_ID not in ({laneIds}) AND B1.R_ID not in ({laneIds}) GROUP BY B1.R_ID'
        # print(query_lm)
        cur.execute(query_lm)
        lmIds = idsCODES(cur.fetchall())
        
        sql_lm = f'select TILE,SUM(LENGTH) from Road where R_ID in ({lmIds}) {tile}'
        
        kml = 0
        cur.execute(sql_lane)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
        cur.execute(sql_road)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
        cur.execute(sql_lm)
        data = cur.fetchall()
        for item in data:
            if item[0] is not None:
                kml += item[1]
        if (kml != 0):
            print(filename,'缺陷',cm_to_km_rounded(kml))
        # conn.commit()
        conn.close()
# process(allPagesPath)
if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument("--datapath",type=str,required=True)
   
    args = parser.parse_args()
    process(args.datapath)
    # processDef(args.datapath)
    # lll =[]
    # for item in lll:
        # processDef(f"D:\\nvm\\6_23_10_09_01_xformat_202403311430_PatchInfo_DefectInfo_202405221730_pure\\6_23_10_09_01_xformat_202403311430_PatchInfo_DefectInfo_202405221730_pure\\{item}")

# python update_all_fieldtype_q.py --datapath=D:\code\python\tool\query_road\6_23_10_09_01_xformat_202403311430_PatchInfo_DefectInfo_202406072200_pure