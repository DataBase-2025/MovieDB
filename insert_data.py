import pandas as pd
import math
from db_conn import open_db, close_db

def clear_all_data(cur, conn):
    print("기존 데이터 삭제 중...")
    for table in ["movie_genre", "movie_nation", "movie_director", "movie", "director", "genre", "nation"]:
        cur.execute(f"DELETE FROM {table}")
    for table in ["movie", "director", "genre", "nation"]:
        cur.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
    conn.commit()
    print("데이터 초기화 완료")

def get_or_insert_cached(cur, table, name, cache):
    if not name or str(name).strip() == "" or (isinstance(name, float) and math.isnan(name)):
        return None
    name = name.strip()
    if name in cache:
        return cache[name]
    cur.execute(f"SELECT {table}_id FROM {table} WHERE name=%s", (name,))
    result = cur.fetchone()
    if result:
        cache[name] = result[f"{table}_id"]
        return cache[name]
    cur.execute(f"INSERT INTO {table} (name) VALUES (%s)", (name,))
    cache[name] = cur.lastrowid
    return cache[name]

def insert_movies_from_excel(file_path):
    conn, cur = open_db()
    clear_all_data(cur, conn)

    df1 = pd.read_excel(file_path, sheet_name=0, header=4)
    df2 = pd.read_excel(file_path, sheet_name=1, header=None)
    df2.columns = df1.columns
    df = pd.concat([df1, df2], ignore_index=True)
    df = df.dropna(subset=['영화명'])

    director_cache, genre_cache, nation_cache = {}, {}, {}
    movie_rows = []
    movie_meta = []

    for idx, row in df.iterrows():
        title = str(row['영화명']).strip()
        english_title = str(row.get('영화명(영문)', '')).strip() if pd.notna(row.get('영화명(영문)')) else None
        year_raw = row.get('제작연도', None)
        try:
            year = int(str(year_raw)[:4]) if pd.notna(year_raw) else None
        except ValueError:
            year = None

        type_val = str(row.get('유형')).strip() if pd.notna(row.get('유형')) else None
        status_val = str(row.get('제작상태')).strip() if pd.notna(row.get('제작상태')) else None
        company_val = str(row.get('제작사')).strip() if pd.notna(row.get('제작사')) else None

        movie_rows.append((title, english_title, year, type_val, status_val, company_val))
        movie_meta.append((idx, row))

    cur.executemany("""
        INSERT INTO movie (title, english_title, open_year, type, status, company)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, movie_rows)

    cur.execute("SELECT movie_id FROM movie ORDER BY movie_id ASC")
    movie_ids = [row['movie_id'] for row in cur.fetchall()]

    movie_director_rows = set()
    movie_genre_rows = set()
    movie_nation_rows = set()

    for i, (idx, row) in enumerate(movie_meta):
        movie_id = movie_ids[i]

        # 감독 split & insert
        directors = [d.strip() for d in str(row.get('감독', '')).split(',') if d.strip()]
        for director in directors:
            director_id = get_or_insert_cached(cur, 'director', director, director_cache)
            if director_id:
                movie_director_rows.add((movie_id, director_id))

        # 장르
        genres = [g.strip() for g in str(row.get('장르', '')).split(',') if g.strip()]
        for genre in genres:
            genre_id = get_or_insert_cached(cur, 'genre', genre, genre_cache)
            if genre_id:
                movie_genre_rows.add((movie_id, genre_id))

        # 국가
        nations = [n.strip() for n in str(row.get('제작국가', '')).split(',') if n.strip()]
        for nation in nations:
            nation_id = get_or_insert_cached(cur, 'nation', nation, nation_cache)
            if nation_id:
                movie_nation_rows.add((movie_id, nation_id))

    cur.executemany("INSERT INTO movie_director (movie_id, director_id) VALUES (%s, %s)", list(movie_director_rows))
    cur.executemany("INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s, %s)", list(movie_genre_rows))
    cur.executemany("INSERT INTO movie_nation (movie_id, nation_id) VALUES (%s, %s)", list(movie_nation_rows))

    conn.commit()
    close_db(conn, cur)
    print("영화 데이터 삽입 완료!")

if __name__ == '__main__':
    insert_movies_from_excel("KOBIS_movie_info.xlsx")