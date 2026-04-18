#--*-- coding:utf-8 --*--
# 背景：23Q2版本高速数据的运营图层数据迁移至高速普通路合并版中
# 输入：A目录sq3文件，B目录SQ3文件。
# 方案：
# 运营图层迁移——直接迁移
# 1.A目录sq3文件中，判断defectinfo图层和patchinfo图层是否存在记录，如果存在记录通过所在的文件名称去B目录找到相同名称的文件。
# 2.逐条插入文件中defectinfo图层和patchinfo图层中。
# 基础数据迁移——补丁点所属数据迁移
# 新增数据迁移：
#  1、读取A目录中SQ3文件内的patchinfo补丁点图层，读取PCINF_TYPE字段为6并且LAYER字段值（通过配置表进行读取对照关系。）FEATURE_ID的值去B目录中查找相同tile号一样的文件，对应的图层中搜索数据。
#  2、
from osgeo import ogr,osr
import time, os, shutil, sqlite3
# 遍历文件
# 数据拷贝，如果存在已拷贝目录删除目录重新拷贝。
# def copy_dir(input, output):
#     if os.path.exists(output):
#         shutil.rmtree(output)
#     shutil.copytree(input, output)
def backup_and_update_defectinfo(defectinfo_folder):
    backup_folder = 'backup'
    os.makedirs(backup_folder, exist_ok=True)

    for root, dirs, files in os.walk(defectinfo_folder):
        for file in files:
            if file.endswith('.sq3'):
                sq3_file_path = os.path.join(root, file)
                backup_file_path = os.path.join(backup_folder, file)
                # print(sq3_file_path)
                # 备份A目录
                os.makedirs(os.path.dirname(backup_file_path), exist_ok=True)
                shutil.copy(sq3_file_path, backup_file_path)
                # 连接A目录下的SQ3文件
                conn = sqlite3.connect(sq3_file_path)
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(DefectInfo)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'flag' not in columns:
                    # 更新DefectInfo图层
                    cursor.execute("ALTER TABLE DefectInfo ADD COLUMN flag INTEGER")
                    conn.commit()

                # 关闭数据库连接
                conn.close()
# 遍历数据目录输出sq3文件列表
def get_sq3_files(directory):
    sq3_files = []
    for root, dirs, files in os.walk(directory):
        # print(root)
        # print(dirs)
        for file in files:
            if file.endswith('.sq3'):
                sq3_files.append(os.path.join(root, file))
                # print(sq3_files)
    return sq3_files
# 读取配置文件，建立为字典。
def GetConfigField(config_file):
    config_data = {}
    with open(config_file, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            # 以逗号为分隔符，将每行分割成键值对
            key, value = row[0], row[1]
            config_data[key] = value
    return config_data
# 查询运营图层中soffset!=eoffset的数据，通过tile和id去两版基准数据中匹配对应的数据。
def extract_and_match_data(config_file, a_folder, b_folder, c_folder):
    # 读取配置文件
    config_data = GetConfigField(config_file)

    # 从配置文件中获取参数
    layer_value = config_data.get("layer_value")
    layer = config_data.get("layer")
    feature_id_name = config_data.get("feature_id_name")

    matched_data = []

    # 获取A目录的SQ3文件列表
    a_sq3_files = get_sq3_files(a_folder)

    for a_sq3_file in a_sq3_files:
        # 连接A目录的SQ3文件
        conn_a = sqlite3.connect(a_sq3_file)
        cursor_a = conn_a.cursor()

        # 查询符合条件的DefectInfo信息
        cursor_a.execute(f"SELECT tile, ID FROM DefectInfo WHERE SOFFSET != EOFFSET AND layer = {layer_value}")
        defectinfo_data = cursor_a.fetchall()

        # 关闭数据库连接
        conn_a.close()

        # 获取B目录的SQ3文件列表
        b_sq3_files = get_sq3_files(b_folder)

        for b_sq3_file in b_sq3_files:
            # 连接B目录的SQ3文件
            conn_b = sqlite3.connect(b_sq3_file)
            cursor_b = conn_b.cursor()

            # 遍历 defectinfo_data 并匹配
            for tile, ID in defectinfo_data:
                cursor_b.execute(
                    f"SELECT * FROM {layer} WHERE {feature_id_name} IN (SELECT {feature_id_name} FROM DefectInfo WHERE tile = ? AND ID = ?)",
                    (tile, ID))
                matched_data.extend(cursor_b.fetchall())

            # 关闭数据库连接
            conn_b.close()

    return matched_data
# def match_data_in_sq3(a_folder, b_folder, layer_value, road_layer, layer_id):
#     matched_data = []
#
#     # 获取A文件夹下的所有SQ3文件
#     a_sq3_files = get_sq3_files(a_folder)
#     b_sq3_files = get_sq3_files(b_folder)
#
#     for a_file in a_sq3_files:
#         # 连接A目录下的SQ3文件
#         conn_a = sqlite3.connect(a_file)
#         cursor_a = conn_a.cursor()
#
#         # 查询符合条件的DefectInfo信息
#         cursor_a.execute(f"SELECT * FROM DefectInfo WHERE SOFFSET != EOFFSET AND layer = {layer_value}")
#         defectinfo_data = cursor_a.fetchall()
#
#         # 关闭A目录下的SQ3文件连接
#         conn_a.close()
#
#         for b_file in b_sq3_files:
#             # 连接B目录下的SQ3文件
#             conn_b = sqlite3.connect(b_file)
#             cursor_b = conn_b.cursor()
#
#             # 查询B目录中的数据，匹配 tile 和 ID 字段
#             for row in defectinfo_data:
#                 tile = row[2]
#                 id = row[7]
#                 cursor_b.execute(f"SELECT * FROM {road_layer} WHERE tile = {tile} AND {layer_id} = {id}")
#                 matched_data.extend(cursor_b.fetchall())
#                 print(matched_data)
#             # 关闭B目录下的SQ3文件连接
#             conn_b.close()
#
#     return matched_data
def main():
    # patchinfo_dir = input("请输入要遍历的目录路径：")
    defectinfo_dir = '/Users/python/pythonclass/homework_data/homework38/out/6_23_02_10_02_xformat_PatchInfo_DefectInfo'
    # copy_dir = input("请输入拷贝目录路径：")
    out_defectinfo = '/Users/python/pythonclass/homework_data/homework38/q4out/6_23_02_10_02_xformat'
    # no_patch_dir = input("输入基准版Q1目录路径：")
    backup_and_update_defectinfo(defectinfo_dir)
    match_data_in_sq3(defectinfo_dir, out_defectinfo, 1, 'road', 'r_id')
    compare_geometry_and_fields()
    print("处理完成。")

if __name__ == "__main__":
    main()