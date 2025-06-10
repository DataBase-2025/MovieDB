from flask import Flask, request, jsonify
from flask_cors import CORS
from db_conn import open_db, close_db

app = Flask(__name__)
CORS(app)

@app.route("/api/movies", methods=["GET"])
def search_movies():
    conn, cur = open_db()

    query = """
        SELECT
            m.movie_id, m.title, m.english_title, m.open_year,
            m.type, m.status, m.company,
            GROUP_CONCAT(DISTINCT d.name) AS directors,
            GROUP_CONCAT(DISTINCT g.name) AS genres,
            GROUP_CONCAT(DISTINCT n.name) AS nations
        FROM movie m
        LEFT JOIN movie_director md ON m.movie_id = md.movie_id
        LEFT JOIN director d ON md.director_id = d.director_id
        LEFT JOIN movie_genre mg ON m.movie_id = mg.movie_id
        LEFT JOIN genre g ON mg.genre_id = g.genre_id
        LEFT JOIN movie_nation mn ON m.movie_id = mn.movie_id
        LEFT JOIN nation n ON mn.nation_id = n.nation_id
        WHERE 1=1
    """

    params = []

    title = request.args.get("title")
    if title:
        query += " AND MATCH(m.title) AGAINST (%s IN BOOLEAN MODE)"
        params.append(title)

    director = request.args.get("director")
    if director:
        query += " AND MATCH(d.name) AGAINST (%s IN BOOLEAN MODE)"
        params.append(director)

    year = request.args.get("year")
    if year and year.isdigit():
        query += " AND m.open_year = %s"
        params.append(year)

    query += " GROUP BY m.movie_id"

    cur.execute(query, params)
    results = cur.fetchall()

    for row in results:
        row["directors"] = row["directors"].split(",") if row["directors"] else []
        row["genres"] = row["genres"].split(",") if row["genres"] else []
        row["nations"] = row["nations"].split(",") if row["nations"] else []

    close_db(conn, cur)
    return jsonify(results)

if __name__ == "__main__":
    app.run(debug=True)
