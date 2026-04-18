import os
import xml.etree.ElementTree as ET
from typing import List

def merge_kml_files(kml_files: List[str], output_file: str) -> None:
    """
    合并多个KML文件到一个KML文件中
    
    Args:
        kml_files: KML文件路径列表
        output_file: 输出文件路径
    """
    # 创建根KML元素
    kml_namespace = {'kml': 'http://www.opengis.net/kml/2.2'}
    
    # 创建新的KML根元素
    root = ET.Element('kml')
    root.set('xmlns', 'http://www.opengis.net/kml/2.2')
    
    # 创建Document元素
    document = ET.SubElement(root, 'Document')
    document.set('id', 'merged_routes')
    
    # 添加文档名称
    name = ET.SubElement(document, 'name')
    name.text = '合并的路线文件'
    
    # 创建文件夹来存储所有路线
    main_folder = ET.SubElement(document, 'Folder')
    folder_name = ET.SubElement(main_folder, 'name')
    folder_name.text = '所有路线'
    
    # 遍历所有KML文件
    for i, kml_file in enumerate(kml_files):
        try:
            # 解析KML文件
            tree = ET.parse(kml_file)
            kml_root = tree.getroot()
            
            # 获取文件名（不含扩展名）作为文件夹名
            file_basename = os.path.splitext(os.path.basename(kml_file))[0]
            
            # 为每个KML文件创建一个文件夹
            route_folder = ET.SubElement(main_folder, 'Folder')
            folder_name = ET.SubElement(route_folder, 'name')
            folder_name.text = f'路线{i+1}: {file_basename}'
            
            # 查找所有的Folder和Placemark元素
            for element in kml_root.findall('.//Folder', kml_namespace):
                # 复制Folder元素
                new_folder = ET.SubElement(route_folder, 'Folder')
                for child in element:
                    new_folder.append(child)
            
            # 查找直接在Document下的Placemark元素
            for element in kml_root.findall('.//Document//Placemark', kml_namespace):
                # 复制Placemark元素
                new_placemark = ET.SubElement(route_folder, 'Placemark')
                for child in element:
                    new_placemark.append(child)
                    
            print(f'成功处理文件: {kml_file}')
            
        except Exception as e:
            print(f'处理文件 {kml_file} 时出错: {str(e)}')
            continue
    
    # 创建格式化的XML输出
    ET.indent(root, space="  ", level=0)
    
    # 保存合并后的KML文件
    tree = ET.ElementTree(root)
    tree.write(output_file, encoding='UTF-8', xml_declaration=True)
    
    print(f'\n合并完成！输出文件: {output_file}')

def batch_merge_kml_from_directory(input_dir: str, output_file: str) -> None:
    """
    从指定目录合并所有KML文件
    
    Args:
        input_dir: 包含KML文件的目录
        output_file: 输出文件路径
    """
    # 查找目录中所有的KML文件
    kml_files = []
    for file in os.listdir(input_dir):
        if file.lower().endswith('.kml'):
            kml_files.append(os.path.join(input_dir, file))
    
    if not kml_files:
        print('未找到KML文件！')
        return
    
    print(f'找到 {len(kml_files)} 个KML文件:')
    for kml_file in kml_files:
        print(f'  - {os.path.basename(kml_file)}')
    
    # 合并文件
    merge_kml_files(kml_files, output_file)

# 使用示例
if __name__ == "__main__":
    # 示例1: 合并指定的KML文件列表
    kml_file_list = [
        '/mnt/data/route1.kml',
        '/mnt/data/route2.kml',
        '/mnt/data/route3.kml'
    ]
    
    # 示例2: 从目录合并所有KML文件
    input_directory = '/mnt/data/kml_files/'  # 包含KML文件的目录
    output_file = '/mnt/data/merged_routes.kml'  # 输出文件路径
    
    # 创建示例目录和文件（用于演示）
    os.makedirs('/mnt/data/kml_files', exist_ok=True)
    
    # 将提供的KML内容保存为示例文件
    sample_kml_content = '''<?xml version="1.0" encoding="UTF-8" standalone="no"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document id="路线规划_20251017180002"><Folder><name>标记点</name><Placemark><name>起始点</name><description><![CDATA[测试路线]]></description><Point><coordinates>119.8030476272106,31.319741274316822</coordinates></Point></Placemark></Folder></Document></kml>'''
    
    # 创建几个示例KML文件
    with open('/mnt/data/kml_files/route1.kml', 'w', encoding='utf-8') as f:
        f.write(sample_kml_content)
    
    with open('/mnt/data/kml_files/route2.kml', 'w', encoding='utf-8') as f:
        f.write(sample_kml_content.replace('测试路线', '测试路线2').replace('route1', 'route2'))
    
    # 执行合并
    batch_merge_kml_from_directory(input_directory, output_file)
