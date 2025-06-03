import pandas as pd 
import math
from db_conn import open_db, close_db

def clear_all_data():
    conn, cur = open_db()
    print("🧹 기존 데이터 삭제 중...")

    cur.execute("DELETE FROM movie_genre")
    cur.execute("DELETE FROM movie_nation")
    cur.execute("DELETE FROM movie")
    cur.execute("DELETE FROM director")
    cur.execute("DELETE FROM genre")
    cur.execute("DELETE FROM nation")
    conn.commit()

    for table in ['movie', 'director', 'genre', 'nation']:
        cur.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
    conn.commit()

    close_db(conn, cur)
    print("✅ 기존 데이터 및 AUTO_INCREMENT 초기화 완료")

def get_or_insert(cur, conn, table, name):
    if not name or str(name).strip() == "" or (isinstance(name, float) and math.isnan(name)):
        return None
    cur.execute(f"SELECT {table}_id FROM {table} WHERE name=%s", (name,))
    result = cur.fetchone()
    if result:
        return result[f"{table}_id"]
    cur.execute(f"INSERT INTO {table} (name) VALUES (%s)", (name,))
    conn.commit()
    return cur.lastrowid

def insert_movies_from_excel(file_path):
    conn, cur = open_db()

    print("📄 엑셀 파일 읽는 중...")
    df1 = pd.read_excel(file_path, sheet_name=0, header=4)
    df2 = pd.read_excel(file_path, sheet_name=1, header=None)

    df2.columns = df1.columns  # 시트2에 컬럼명 부여

    print(f"📄 시트1 행 수: {len(df1)}")
    print(f"📄 시트2 행 수: {len(df2)}")

    df = pd.concat([df1, df2], ignore_index=True)
    print(f"🔀 병합된 총 행 수: {len(df)}")

    df = df.dropna(subset=['영화명'])  # 영화명 없는 데이터 제거
    print(f"✅ 유효한 영화명 수: {len(df)}")

    for idx, row in df.iterrows():
        title = row['영화명']
        year_raw = row.get('제작연도', None)
        try:
            year = int(str(year_raw)[:4]) if pd.notna(year_raw) else None
        except ValueError:
            year = None

        director_id = get_or_insert(cur, conn, 'director', row.get('감독'))
        cur.execute("INSERT INTO movie (title, open_year, director_id) VALUES (%s, %s, %s)", (title, year, director_id))
        conn.commit()
        movie_id = cur.lastrowid

        genres = str(row.get('장르', '')).split(', ')
        for genre in genres:
            genre_id = get_or_insert(cur, conn, 'genre', genre)
            if genre_id:
                cur.execute("INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s, %s)", (movie_id, genre_id))

        nations = str(row.get('제작국가', '')).split(', ')
        for nation in nations:
            nation_id = get_or_insert(cur, conn, 'nation', nation)
            if nation_id:
                cur.execute("INSERT INTO movie_nation (movie_id, nation_id) VALUES (%s, %s)", (movie_id, nation_id))

        if idx % 1000 == 0:
            print(f"📦 {idx}/{len(df)} 행 삽입 중...")

    conn.commit()
    close_db(conn, cur)
    print("🎉 모든 영화 정보 삽입 완료!")

if __name__ == '__main__':
    clear_all_data()
    insert_movies_from_excel("KOBIS_movie_info.xlsx")
