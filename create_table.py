from db_conn import open_db, close_db

def create_tables():
    conn, cur = open_db()

    cur.execute("DROP TABLE IF EXISTS movie_genre, movie_nation, movie_director, movie, director, genre, nation")

    cur.execute("""
        CREATE TABLE director (
            director_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE nation (
            nation_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE genre (
            genre_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE movie (
            movie_id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            english_title VARCHAR(255),
            open_year INT,
            type VARCHAR(100),
            status VARCHAR(100),
            company VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE movie_director (
            movie_id INT,
            director_id INT,
            PRIMARY KEY (movie_id, director_id),
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
            FOREIGN KEY (director_id) REFERENCES director(director_id)
        )
    """)

    cur.execute("""
        CREATE TABLE movie_genre (
            movie_id INT,
            genre_id INT,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
            FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
        )
    """)

    cur.execute("""
        CREATE TABLE movie_nation (
            movie_id INT,
            nation_id INT,
            PRIMARY KEY (movie_id, nation_id),
            FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
            FOREIGN KEY (nation_id) REFERENCES nation(nation_id)
        )
    """)

    cur.execute("CREATE INDEX idx_movie_title ON movie(title)")
    cur.execute("CREATE INDEX idx_movie_open_year ON movie(open_year)")
    cur.execute("CREATE INDEX idx_movie_type ON movie(type)")
    cur.execute("CREATE INDEX idx_director_name ON director(name)")
    cur.execute("CREATE INDEX idx_genre_name ON genre(name)")
    cur.execute("CREATE INDEX idx_nation_name ON nation(name)")
        
    cur.execute("CREATE INDEX idx_movie_director_movie ON movie_director(movie_id, director_id)")
    cur.execute("CREATE INDEX idx_movie_genre_movie ON movie_genre(movie_id, genre_id)")
    cur.execute("CREATE INDEX idx_movie_nation_movie ON movie_nation(movie_id, nation_id)")
        
    cur.execute("CREATE FULLTEXT INDEX ft_movie_title ON movie(title)")
    cur.execute("CREATE FULLTEXT INDEX ft_director_name ON director(name)")
    conn.commit()
    close_db(conn, cur)
    print("테이블 및 인덱스 생성 완료!")

if __name__ == '__main__':
    create_tables()
