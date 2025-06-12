from flask import Flask, request, jsonify
from flask_cors import CORS
from db_conn import open_db, close_db

app = Flask(__name__)
CORS(app, origins=["http://localhost:5174"])

@app.route("/api/movies", methods=["GET"])
def search_movies():
    conn, cur = open_db()

    # 공통 필터
    base_filter = """
        FROM movie m
        LEFT JOIN movie_director md ON m.movie_id = md.movie_id
        LEFT JOIN director d ON md.director_id = d.director_id
        LEFT JOIN movie_genre mg ON m.movie_id = mg.movie_id
        LEFT JOIN genre g ON mg.genre_id = g.genre_id
        LEFT JOIN movie_nation mn ON m.movie_id = mn.movie_id
        LEFT JOIN nation n ON mn.nation_id = n.nation_id
        WHERE 1=1
    """

    filters = ""
    params = []

    title = request.args.get("title")
    if title:
        filters += " AND MATCH(m.title) AGAINST (%s IN BOOLEAN MODE)"
        params.append(title)

    director = request.args.get("director")
    if director:
        filters += " AND MATCH(d.name) AGAINST (%s IN BOOLEAN MODE)"
        params.append(director)

    year = request.args.get("year")
    if year and year.isdigit():
        filters += " AND m.open_year = %s"
        params.append(year)

    # 페이지 번호 (기본값: 1)
    page = int(request.args.get("page", 1))
    per_page = 10  # 고정값
    offset = (page - 1) * per_page

    # 총 개수 쿼리
    count_query = f"""
        SELECT COUNT(DISTINCT m.movie_id) AS total
        {base_filter} {filters}
    """
    cur.execute(count_query, params)
    total_items = cur.fetchone()["total"]
    total_pages = (total_items + per_page - 1) // per_page

    # 실제 데이터 쿼리
    data_query = f"""
        SELECT
            m.movie_id, m.title, m.english_title, m.open_year,
            m.type, m.status, m.company,
            GROUP_CONCAT(DISTINCT d.name) AS directors,
            GROUP_CONCAT(DISTINCT g.name) AS genres,
            GROUP_CONCAT(DISTINCT n.name) AS nations
        {base_filter} {filters}
        GROUP BY m.movie_id
        LIMIT %s OFFSET %s
    """
    cur.execute(data_query, params + [per_page, offset])
    results = cur.fetchall()

    for row in results:
        row["directors"] = row["directors"].split(",") if row["directors"] else []
        row["genres"] = row["genres"].split(",") if row["genres"] else []
        row["nations"] = row["nations"].split(",") if row["nations"] else []

    close_db(conn, cur)

    return jsonify({
        "data": results,
        "pagination": {
            "total_pages": total_pages,
            "current_page": page,
            "total_items": total_items,
            "per_page": per_page
        }
    })

if __name__ == "__main__":
    app.run(debug=True)
