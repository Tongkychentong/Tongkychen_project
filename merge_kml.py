# coding = utf-8
import os
from fastkml import kml
from lxml import etree

def merge_kml_files_from_directory(input_directory, output_file):
    ns = '{http://www.opengis.net/kml/2.2}'

    # 创建一个新的 KML 对象
    merged_kml = kml.KML()
    merged_doc = kml.Document(ns, '1', 'Merged Document', 'A merged KML document')
    merged_kml.append(merged_doc)

    # 遍历目录中的所有 KML 文件
    for filename in os.listdir(input_directory):
        if filename.endswith(".kml"):
            file_path = os.path.join(input_directory, filename)
            with open(file_path, 'rt', encoding='utf-8') as f:
                content = f.read()
                doc = kml.KML()
                doc.from_string(content)
                for feature in doc.features():
                    if isinstance(feature, kml.Document):
                        for placemark in feature.features():
                            if isinstance(placemark, kml.Placemark):
                                if hasattr(placemark, 'geometry') and placemark.geometry is not None:
                                    merged_doc.append(placemark)
                                else:
                                    for track in placemark.features():
                                        if isinstance(track, kml.GxTrack):
                                            merged_doc.append(track)

    # 将合并后的 KML 写入文件
    with open(output_file, 'wb') as f:
        f.write(merged_kml.to_string(prettyprint=True).encode('utf-8'))

# 输入目录路径
input_directory = '/Users/mi/data/KML/in'  # 修改为你的输入目录路径

# 输出文件
output_file = '/Users/mi/data/KML/out/merged_output.kml'

merge_kml_files_from_directory(input_directory, output_file)