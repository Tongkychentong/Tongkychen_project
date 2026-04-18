import os
import sqlite3
import argparse
from pathlib import Path

def get_sq3_files(directory):
    """递归遍历目录，返回所有.sq3文件的字典，键为文件名，值为文件路径。"""
    sq3_files = {}
    for path in Path(directory).rglob('*.sq3'):
        if path.name not in sq3_files:
            sq3_files[path.name] = path
        else:
            print(f"警告: 发现同名文件 {path}，将只保留第一个匹配项。")
    return sq3_files

def get_table_name(cursor):
    """动态获取表名，忽略大小写，寻找 'road' 或 'Road'"""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        if table.lower() == 'road':
            return table
    return None

def get_id_column(cursor, table_name):
    """动态获取表中的 id 字段名（忽略大小写差异）"""
    cursor.execute(f'PRAGMA table_info("{table_name}");')
    columns = [col[1] for col in cursor.fetchall()]
    for col in columns:
        # 支持各种可能的ID字段名
        if col.lower() in ('r_id', 'roadid', 'road_id', 'id'):
            return col
    return None

def get_r_ids(db_path):
    """提取数据库图层中所有的 ID，返回字典 {字符串ID: 原始ID}，以防数据类型不一致。"""
    r_ids_map = {}
    id_col = None
    table_name = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 动态获取真实的表名 (Road 或 road)
        table_name = get_table_name(cursor)
        
        if not table_name:
            print(f"警告: {db_path} 中不存在 Road 或 road 表。")
            return r_ids_map, None, None
            
        # 动态获取字段名
        id_col = get_id_column(cursor, table_name)
        if not id_col:
            # 如果没找到，打印出所有的列名帮助调试
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            all_cols = [col[1] for col in cursor.fetchall()]
            print(f"警告: 在 {db_path} 的 {table_name} 表中找不到 r_id 字段。可用的字段有: {all_cols}")
            return r_ids_map, None, table_name
            
        cursor.execute(f'SELECT "{id_col}" FROM "{table_name}";')
        rows = cursor.fetchall()
        for row in rows:
            if row[0] is not None:
                # 统一转为字符串作为键
                r_ids_map[str(row[0])] = row[0]
                
    except sqlite3.Error as e:
        print(f"读取数据库出错 {db_path}: {e}")
    except Exception as e:
        print(f"发生未知错误 {db_path}: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
    return r_ids_map, id_col, table_name

def export_diff_data(src_db_path, dest_db_path, diff_r_ids, id_col, table_name):
    """将存在差异的数据导出到新的数据库中。"""
    error_occurred = False
    try:
        src_conn = sqlite3.connect(src_db_path)
        src_cursor = src_conn.cursor()
        
        dest_conn = sqlite3.connect(dest_db_path)
        dest_cursor = dest_conn.cursor()
        
        # 复制表及其索引等结构
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name='{table_name}' AND sql IS NOT NULL;")
        schemas = src_cursor.fetchall()
        for schema in schemas:
            dest_cursor.execute(schema[0])
        
        # 获取列名以构建INSERT语句
        src_cursor.execute(f'PRAGMA table_info("{table_name}");')
        columns = [col[1] for col in src_cursor.fetchall()]
        col_names = ','.join(f'"{col}"' for col in columns)
        val_placeholders = ','.join('?' for _ in columns)
        insert_sql = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({val_placeholders})'
        
        # 分批查询和插入以避免SQLite参数数量限制
        chunk_size = 900
        for i in range(0, len(diff_r_ids), chunk_size):
            chunk = diff_r_ids[i:i + chunk_size]
            placeholders = ','.join('?' for _ in chunk)
            query = f'SELECT * FROM "{table_name}" WHERE "{id_col}" IN ({placeholders})'
            
            src_cursor.execute(query, chunk)
            rows = src_cursor.fetchall()
            
            if rows:
                dest_cursor.executemany(insert_sql, rows)
                
        dest_conn.commit()
            
    except sqlite3.Error as e:
        print(f"导出数据出错 {dest_db_path}: {e}")
        error_occurred = True
    finally:
        if 'src_conn' in locals():
            src_conn.close()
        if 'dest_conn' in locals():
            dest_conn.close()
            
    # 如果发生错误，清理不完整的文件
    if error_occurred and os.path.exists(dest_db_path):
        os.remove(dest_db_path)

def process_directories(dir_a, dir_b, output_dir):
    dir_a_path = Path(dir_a)
    dir_b_path = Path(dir_b)
    out_dir_path = Path(output_dir)
    
    if not dir_a_path.exists():
        print(f"源目录 A 不存在: {dir_a}")
        return
    if not dir_b_path.exists():
        print(f"目标目录 B 不存在: {dir_b}")
        return
        
    # 创建输出目录
    out_dir_path.mkdir(parents=True, exist_ok=True)
    
    print(f"正在扫描源目录 A: {dir_a}")
    files_a = get_sq3_files(dir_a)
    print(f"在目录 A 中找到 {len(files_a)} 个 .sq3 文件。")
    
    print(f"正在扫描目标目录 B: {dir_b}")
    files_b = get_sq3_files(dir_b)
    print(f"在目录 B 中找到 {len(files_b)} 个 .sq3 文件。")
    
    # 按照文件名匹配
    matched_files = set(files_a.keys()).intersection(set(files_b.keys()))
    print(f"找到 {len(matched_files)} 个匹配的同名文件。")
    
    target_id = "2022215707635544064"
    
    for filename in matched_files:
        path_a = files_a[filename]
        path_b = files_b[filename]
        
        r_ids_a_map, id_col_a, table_name_a = get_r_ids(path_a)
        r_ids_b_map, id_col_b, table_name_b = get_r_ids(path_b)
        
        # 打印文件A读取到的ID数量
        print(f"\n文件 {filename}:")
        print(f"  - 目录A中表 '{table_name_a}' 包含记录数: {len(r_ids_a_map)}")
        print(f"  - 目录B中表 '{table_name_b}' 包含记录数: {len(r_ids_b_map)}")
        
        if not id_col_a or not table_name_a:
            continue
            
        if target_id in r_ids_a_map:
            print(f"  [调试] 在源文件 A 中找到了目标记录: {target_id}")
            if target_id in r_ids_b_map:
                print(f"  [调试] 该记录在目标文件 B 中也存在，因此不算差异。")
            else:
                print(f"  [调试] 该记录在目标文件 B 中缺失，准备导出！")
        else:
            if "556805421" in filename:
                print(f"  [严重警告] 在文件 A ({filename}) 中竟然没有找到 ID {target_id}！")
                print(f"  [诊断] 请检查文件 A 中的 {table_name_a} 表是否真的包含这条记录，或者字段名是否异常。")
                if r_ids_a_map:
                    # 打印前5个ID作为样本
                    sample_ids = list(r_ids_a_map.keys())[:5]
                    print(f"  [诊断] 文件 A 中提取到的部分 ID 样本: {sample_ids}")
        
        # A有B无的差异数据
        diff_keys = set(r_ids_a_map.keys()) - set(r_ids_b_map.keys())
        
        if diff_keys:
            diff_r_ids = [r_ids_a_map[k] for k in diff_keys]
            print(f"  发现 {len(diff_r_ids)} 个新增的 ID (A有B无)。")
            output_filepath = out_dir_path / filename
            export_diff_data(path_a, output_filepath, diff_r_ids, id_col_a, table_name_a)
            print(f"  已导出差异数据至: {output_filepath}")
        else:
            print(f"  未发现 A有B无 的差异数据。")
            
    print("\n处理完成！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="服务区数据 ID 比对与迁移预处理工具")
    parser.add_argument("-a", "--dir_a", required=True, help="源目录 A 的路径")
    parser.add_argument("-b", "--dir_b", required=True, help="目标目录 B 的路径")
    parser.add_argument("-o", "--output_dir", required=True, help="结果输出独立目录的路径")
    
    args = parser.parse_args()
    
    process_directories(args.dir_a, args.dir_b, args.output_dir)
