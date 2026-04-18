import os
import sqlite3
from lxml import etree
from datetime import datetime

# --- 数据库表结构定义 ---
def create_database(db_path):
    """创建SQLite数据库文件及表结构"""
    if os.path.exists(db_path):
        os.remove(db_path)
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 元信息表
    cursor.execute('''
    CREATE TABLE kml_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_filename TEXT NOT NULL,
        conversion_time TEXT NOT NULL
    )
    ''')

    # 标记点表
    cursor.execute('''
    CREATE TABLE waypoints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        geometry_type TEXT NOT NULL,
        longitude REAL,
        latitude REAL,
        altitude REAL
    )
    ''')

    # 路线/多边形表
    cursor.execute('''
    CREATE TABLE geometries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        geometry_type TEXT NOT NULL,
        coordinates TEXT NOT NULL
    )
    ''')
    
    conn.commit()
    conn.close()
    print(f"[INFO] 数据库结构已创建: {db_path}")

def parse_kml_file(kml_path):
    """解析KML文件，提取点和几何信息"""
    print(f"  [DEBUG] 开始解析文件: {os.path.basename(kml_path)}")
    
    # 尝试不同编码
    encodings_to_try = ['utf-8', 'gbk', 'latin1']
    content = None
    for enc in encodings_to_try:
        try:
            with open(kml_path, 'r', encoding=enc) as f:
                content = f.read()
            print(f"  [INFO] 使用 {enc} 编码读取文件成功。")
            break
        except UnicodeDecodeError:
            print(f"  [WARN] {enc} 编码失败，尝试下一个...")
    
    if content is None:
        print(f"  [ERROR] 所有编码均失败，无法读取文件: {kml_path}")
        return None, None

    try:
        # 解析XML并处理命名空间
        root = etree.fromstring(content.encode('utf-8'))
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        
        # 收集所有Placemark
        placemarks = root.xpath('.//kml:Placemark', namespaces=ns)
        print(f"  [INFO] 发现 {len(placemarks)} 个 Placemark。")
        
        points = []      # 用于存储点 (Point)
        geometries = []   # 用于存储线/面 (LineString, Polygon)
        
        for i, placemark in enumerate(placemarks):
            name = placemark.find('kml:name', ns)
            name = name.text.strip() if name is not None else f"未命名_{i}"
            
            desc = placemark.find('kml:description', ns)
            desc = desc.text.strip() if desc is not None else ""
            
            # 检查几何类型
            point_elem = placemark.find('kml:Point', ns)
            line_elem = placemark.find('kml:LineString', ns)
            poly_elem = placemark.find('kml:Polygon', ns)
            
            if point_elem is not None:
                print(f"  [INFO] 解析点: {name}")
                coords_text = point_elem.find('kml:coordinates', ns)
                if coords_text is not None and coords_text.text:
                    try:
                        lon, lat, alt = map(float, coords_text.text.split(',')[:3])
                        points.append({
                            'name': name,
                            'description': desc,
                            'geometry_type': 'Point',
                            'lon': lon,
                            'lat': lat,
                            'alt': alt
                        })
                    except ValueError as e:
                        print(f"  [WARN] 点坐标解析失败 ({name}): {e}")
                else:
                    print(f"  [WARN] 点坐标缺失 ({name})")
                    
            elif line_elem is not None or poly_elem is not None:
                elem = line_elem if line_elem is not None else poly_elem
                geo_type = 'LineString' if line_elem is not None else 'Polygon'
                print(f"  [INFO] 解析{geo_type}: {name}")
                
                coords_text = elem.find('kml:coordinates', ns)
                if coords_text is not None and coords_text.text:
                    try:
                        # 移除空格并分割坐标对
                        coord_pairs = [pair.strip() for pair in coords_text.text.split()]
                        geometries.append({
                            'name': name,
                            'description': desc,
                            'geometry_type': geo_type,
                            'coordinates': ' '.join(coord_pairs)
                        })
                    except ValueError as e:
                        print(f"  [WARN] {geo_type}坐标解析失败 ({name}): {e}")
                else:
                    print(f"  [WARN] {geo_type}坐标缺失 ({name})")
                    
            else:
                print(f"  [INFO] 跳过未知几何类型 ({name})")
                
        return points, geometries
        
    except etree.XMLSyntaxError as e:
        print(f"  [ERROR] XML语法错误: {e}")
        return None, None
    except Exception as e:
        print(f"  [ERROR] 解析过程中发生未知错误: {e}")
        return None, None

def process_single_kml(kml_path, db_path):
    """处理单个KML文件"""
    print(f"  --> 开始处理: {os.path.basename(kml_path)}")
    
    points, geometries = parse_kml_file(kml_path)
    
    if points is None or geometries is None:
        print(f"  --> [跳过] 文件解析失败，未生成数据库.")
        return

    create_database(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 写入元信息
    cursor.execute(
        "INSERT INTO kml_metadata (original_filename, conversion_time) VALUES (?, ?)",
        (os.path.basename(kml_path), datetime.now().isoformat())
    )

    # 写入点数据
    for pt in points:
        cursor.execute(
            "INSERT INTO waypoints (name, description, geometry_type, longitude, latitude, altitude) VALUES (?, ?, ?, ?, ?, ?)",
            (pt['name'], pt['description'], pt['geometry_type'], pt['lon'], pt['lat'], pt['alt'])
        )

    # 写入几何数据
    for geom in geometries:
        cursor.execute(
            "INSERT INTO geometries (name, description, geometry_type, coordinates) VALUES (?, ?, ?, ?)",
            (geom['name'], geom['description'], geom['geometry_type'], geom['coordinates'])
        )
    
    conn.commit()
    conn.close()
    print(f"  --> [成功] 转换完成: {os.path.basename(kml_path)} (点: {len(points)}, 几何: {len(geometries)})\n")

def batch_convert_kml_to_sq3():
    """主函数：交互式获取目录并执行批量转换"""
    print("--- KML 到 SQ3 批量转换工具 (终极版) ---")
    
    # 获取输入目录
    while True:
        input_dir = input("请输入包含KML文件的目录路径: ").strip().strip('"')
        if os.path.isdir(input_dir):
            break
        else:
            print("错误：输入的路径不是一个有效的目录，请重新输入。")

    # 获取输出目录
    while True:
        output_dir = input("请输入用于保存.sq3文件的目录路径 (如果不存在将自动创建): ").strip().strip('"')
        if not output_dir:
            print("错误：输出路径不能为空，请重新输入。")
            continue
        try:
            os.makedirs(output_dir, exist_ok=True)
            break
        except Exception as e:
            print(f"错误：无法创建或访问输出目录 '{output_dir}'。原因: {e}")
            print("请检查路径权限或选择另一个目录。")

    print("\n--- 开始批量转换 ---")
    
    kml_files_found = False
    for filename in os.listdir(input_dir):
        if filename.lower().endswith('.kml'):
            kml_files_found = True
            kml_path = os.path.join(input_dir, filename)
            sq3_filename = os.path.splitext(filename)[0] + '.sq3'
            sq3_path = os.path.join(output_dir, sq3_filename)
            
            process_single_kml(kml_path, sq3_path)
            
    if not kml_files_found:
        print(f"在目录 '{input_dir}' 中未找到任何 .kml 文件。")
    else:
        print("\n--- 批量转换完成！ ---")
        print(f"所有 .sq3 文件已保存至: {output_dir}")

if __name__ == "__main__":
    batch_convert_kml_to_sq3()
