# db_conn.py
import pymysql
from pymysql.constants.CLIENT import MULTI_STATEMENTS

def open_db(dbname='movie_info'):
    conn = pymysql.connect(
        host='localhost',
        user='root',
        passwd='12forever0309@',  # ← 너의 MySQL 비번으로 바꿔줘
        db=dbname,
        charset='utf8mb4',
        client_flag=MULTI_STATEMENTS
    )
    cur = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cur

def close_db(conn, cur):
    cur.close()
    conn.close()
