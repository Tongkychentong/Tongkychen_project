"""生产工具：计算运营图层所在道路长度，聚合长度统计里程。
1、输入：分文件夹的图幅数据
2、输出： 分tile统计的里程表"""
import shutil
import sqlite3
from math import radians, cos, sin, asin, sqrt
from osgeo import ogr, osr

osr.UseExceptions()

# 拷贝数据到另一个目录
def backup_database(db_path, backup_path):
    shutil.copyfile(db_path, backup_path)
    print(f"Backup created at {backup_path}")


# 删除匹配记录
def delete_matching_records(new_db_path, old_db_path):
    # 链接两个数据库
    conn_new = sqlite3.connect(new_db_path)
    conn_old = sqlite3.connect(old_db_path)

    try:
        cursor_new = conn_new.cursor()
        cursor_old = conn_old.cursor()

        # Fetch records from old.sq3
        cursor_old.execute("SELECT FEATURE_ID, ASSIST_ID, CHECK_ID, ERR_CODE, GEOMETRY FROM auto_check")
        old_records = cursor_old.fetchall()

        # Prepare SQL DELETE statement
        delete_statement = """
        DELETE FROM auto_check 
        WHERE FEATURE_ID = ? AND ASSIST_ID = ? AND CHECK_ID = ? AND ERR_CODE = ? AND GEOMETRY = ?
        """

        # Delete matching records in new.sq3
        for record in old_records:
            cursor_new.execute(delete_statement, record)

        # Commit the changes
        conn_new.commit()
        print("Matching records deleted from new.sq3")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn_new.rollback()

    finally:
        conn_new.close()
        conn_old.close()


if __name__ == "__main__":
    new_db_path = 'new.sq3'
    old_db_path = 'old.sq3'
    backup_path = 'new_backup.sq3'

    # Step 1: Backup the new.sq3 file
    backup_database(new_db_path, backup_path)

    # Step 2: Delete matching records from new.sq3
    delete_matching_records(new_db_path, old_db_path)