from db_conn import open_db, close_db

def create_tables():
    conn, cur = open_db()

    cur.execute("DROP TABLE IF EXISTS movie_genre, movie_nation, movie, director, genre, nation")

    cur.execute("""
        CREATE TABLE director (
            director_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )""")

    cur.execute("""
        CREATE TABLE nation (
            nation_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) UNIQUE
        )""")

    cur.execute("""
        CREATE TABLE genre (
            genre_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) UNIQUE
        )""")

    cur.execute("""
        CREATE TABLE movie (
            movie_id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            open_year INT,
            director_id INT,
            FOREIGN KEY (director_id) REFERENCES director(director_id)
        )""")

    cur.execute("""
        CREATE TABLE movie_genre (
            movie_id INT,
            genre_id INT,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
            FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
        )""")

    cur.execute("""
        CREATE TABLE movie_nation (
            movie_id INT,
            nation_id INT,
            PRIMARY KEY (movie_id, nation_id),
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
            FOREIGN KEY (nation_id) REFERENCES nation(nation_id)
        )""")

    conn.commit()
    close_db(conn, cur)
    print("✅ 테이블 생성 완료")

if __name__ == '__main__':
    create_tables()
