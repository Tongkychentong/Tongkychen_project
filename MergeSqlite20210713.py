#--*-- coding:utf-8 --*--
import os
import time
import gdal
import ogr
import shutil
import sqlite3
import traceback

#导入GUI相关库
from tkinter import *
from tkinter import filedialog
from tkinter import font

#读取指定目录下文件列表，可跨目录                
def GetFileList(dir, fileList):
    if os.path.isfile(dir):
        fileList.append(dir)
    elif os.path.isdir(dir):  
        for s in os.listdir(dir):
            newDir = os.path.join(dir,s)
            GetFileList(newDir, fileList)  
    return fileList                

def Folderpath():
    in_path = filedialog.askdirectory()
    if in_path:
        v1.set(in_path)

def Folderpath_1():
    out_path = filedialog.askdirectory()
    if out_path:
        v2.set(out_path)

def main():
    try:
        input_path = v1.get()
        output_path = v2.get()

        start_time = time.time()
        print ('Start',time.strftime('%H:%M:%S',time.localtime(time.time())))
        text.insert(END,('Start',time.strftime('%H:%M:%S',time.localtime(time.time()))),'tag')
        text.insert(END,'\n')
        text.see(END)
        text.update()

        print ("输入目录："+str(input_path))
        print ("输出目录："+str(output_path))
        text.insert(END,("输入目录："+str(input_path)),'tag')
        text.insert(END,'\n')
        text.insert(END,("输出目录："+str(output_path)),'tag')
        text.insert(END,'\n')
        text.see(END)
        text.update()

        # 为了支持中文路径，请添加下面这句代码
        gdal.SetConfigOption('GDAL_FILENAME_IS_UTF8','NO')
        # 为了使属性表字段支持中文，请添加下面这句
        gdal.SetConfigOption('SQLite_ENCODING','')
        # 注册所有的驱动
        ogr.RegisterAll()
        #数据格式的驱动
        driver = ogr.GetDriverByName("SQLite")

        list = GetFileList(input_path, [])

        sqlite_out = output_path + "\\HD30_Merge.sq3"
        if os.path.exists(sqlite_out):
            os.remove(sqlite_out)
        if not os.path.isdir(output_path):
            os.makedirs(output_path)

        task_coun = 0 #任务数初始值
        for file in list:
            file1 = os.path.basename(file)
            #***********读取sqlite文件*****************
            if file.endswith('.sq3') or file.endswith('.sqlite'):
                task_coun += 1
                if task_coun == 1:
                    shutil.copy (file, sqlite_out)
                    print ("选取%s做为基准表" % (file1))
                    text.insert(END, ("选取%s做为基准表 \n" % (file1)), 'tag')
                    text.see(END)
                    text.update()

                    merge_table = []
                    merge_sq3 = sqlite3.connect(sqlite_out)
                    c = merge_sq3.cursor()
                    sql_merge_table = c.execute("SELECT * FROM sqlite_master WHERE type = 'table'")
                    for row in sql_merge_table:
                        table_name = row[1].lower()
                        if table_name != 'geometry_columns':
                            merge_table.append(table_name)
                    print ("合并列表：" + str(merge_table))
                    text.insert(END, ("合并列表：" + str(merge_table) + "\n"), 'tag')
                    text.see(END)
                    text.update()
                else:
                    c.execute("ATTACH DATABASE '" + file + "' AS tasksq3")
                    c.execute('BEGIN')
                    print("合并库：%s" % (file1))
                    text.insert(END, ("合并库：%s \n" % (file1)), 'tag')
                    text.see(END)
                    text.update()
                    sql_task_table = c.execute("SELECT * FROM tasksq3.sqlite_master WHERE type = 'table'")
                    for row in sql_task_table.fetchall():
                        table_name = row[1].lower()
                        if table_name in merge_table:
                            print ("合表：%s \n" % (table_name))
                            text.insert(END, ("合表：%s \n" % (table_name)), 'tag')
                            text.see(END)
                            text.update()
                            combine = "insert into %s select * from tasksq3.%s" % (table_name, table_name)
                            c.execute(combine)
                    merge_sq3.commit()
                    c.execute("detach database tasksq3")
        merge_sq3.close()
        print ("Combine Finished")
        text.insert(END, 'Combine Finished \n', 'tag')
        print('************************************************')
        text.insert(END, '************************************************ \n', 'tag')

        end_time =time.time()
        print ('End',time.strftime('%H:%M:%S',time.localtime(time.time())))
        print ('用时：%s Seconds'%(end_time-start_time))

        text.insert(END,('End',time.strftime('%H:%M:%S',time.localtime(time.time()))),'tag')
        text.insert(END,'\n')
        text.insert(END,('用时：%s Seconds'%(end_time-start_time)),'tag')
        text.insert(END,'\n')

        text.see(END)
        text.update()

        frameT.quit
    except:
        print(traceback.format_exc())
        text.insert(END, (traceback.format_exc()), 'tag')

if __name__ == "__main__":
    
    #创建窗口
    frameT = Tk()
    #窗口大小
    frameT.geometry('600x400+400+200')
    #标题
    frameT.title('HD30数据处理')

    frame0 = Frame(frameT)
    frame0.pack(padx=10, pady=10)  # 设置外边距
    frame = Frame(frameT)
    frame.pack(padx=10, pady=10)  # 设置外边距
    frame_1 = Frame(frameT)
    frame_1.pack(padx=10, pady=10)  # 设置外边距
    frame1 = Frame(frameT)
    frame1.pack(padx=10, pady=10)

    text = Text(frameT)
    text.pack(fill=X, side=BOTTOM)

    ft = font.Font(family='微软雅黑',size=10)
    text.tag_add('tag',END)
    text.tag_config('tag',foreground = 'blue',background='yellow',font = ft)

    setpathtext = Label(frame0,text="路径设置",fg="black",font=("宋体",12)).pack(fill=X, side=LEFT)
    label_in = Label(frame,text="输入:",fg="black",font=("宋体",12)).pack(fill=X, side=LEFT)
    label_out = Label(frame_1,text="输出:",fg="black",font=("宋体",12)).pack(fill=X, side=LEFT)

    v1 = StringVar()
    v2 = StringVar()
    ent = Entry(frame, width=50, textvariable=v1).pack(fill=X, side=LEFT)  # x方向填充,靠左
    ent = Entry(frame_1, width=50, textvariable=v2).pack(fill=X, side=LEFT)  # x方向填充,靠左


    btn = Button(frame, width=20, text='浏览...', font=("宋体", 12), command=Folderpath).pack(fill=X, padx=20)
    btn_1 = Button(frame_1, width=20, text='浏览...', font=("宋体", 12), command=Folderpath_1).pack(fill=X, padx=20)
    ext = Button(frame1, width=10, text='运行', font=("宋体", 14), command=main).pack(fill=X, side=LEFT)
    etb = Button(frame1, width=10, text='退出', font=("宋体", 14), command=frameT.destroy).pack(fill=Y, padx=80)

    frameT.mainloop()

    
