from flask import Flask, request, jsonify
from flask_cors import CORS
from db_conn import open_db, close_db

app = Flask(__name__)
CORS(app, origins=["http://localhost:5173"])

@app.route("/api/movies", methods=["GET"])
def search_movies():
    conn, cur = open_db()

    filters = []
    params = []

    # 디버깅: 받은 파라미터 출력
    print("=== 받은 파라미터 ===")
    for key, value in request.args.items():
        print(f"{key}: {value}")
    print("==================")

    # 제목, 감독
    title = request.args.get("title")
    if title:
        print(f"제목 필터 적용: {title}")
        filters.append("MATCH(m.title) AGAINST (%s IN BOOLEAN MODE)")
        params.append(title)

    director = request.args.get("director")
    if director:
        filters.append("EXISTS (SELECT 1 FROM movie_director md JOIN director d ON md.director_id = d.director_id WHERE md.movie_id = m.movie_id AND MATCH(d.name) AGAINST (%s IN BOOLEAN MODE))")
        params.append(director)

    # 연도 필터
    year = request.args.get("year")
    if year and year.isdigit():
        filters.append("m.open_year = %s")
        params.append(year)

    open_start_year = request.args.get("open_start_year")
    if open_start_year and open_start_year.isdigit():
        filters.append("m.open_year >= %s")
        params.append(open_start_year)

    open_end_year = request.args.get("open_end_year")
    if open_end_year and open_end_year.isdigit():
        filters.append("m.open_year <= %s")
        params.append(open_end_year)

    # 장르
    genre_str = request.args.get("genres")
    if genre_str:
        genre_list = [g.strip() for g in genre_str.split(",") if g.strip()]
        if genre_list:
            filters.append(
                "EXISTS (SELECT 1 FROM movie_genre mg JOIN genre g ON mg.genre_id = g.genre_id WHERE mg.movie_id = m.movie_id AND g.name IN (%s))"
                % ",".join(["%s"] * len(genre_list))
            )
            params.extend(genre_list)

    # 국적
    nation_str = request.args.get("nations") or request.args.get("nation")
    if nation_str:
        nation_list = [n.strip() for n in nation_str.split(",") if n.strip()]
        filters.append("EXISTS (SELECT 1 FROM movie_nation mn JOIN nation n ON mn.nation_id = n.nation_id WHERE mn.movie_id = m.movie_id AND n.name IN (%s))" % ",".join(["%s"] * len(nation_list)))
        params.extend(nation_list)

    # 유형
    type_str = request.args.get("type")
    if type_str:
        type_list = [t.strip() for t in type_str.split(",") if t.strip()]
        filters.append("m.type IN (%s)" % ",".join(["%s"] * len(type_list)))
        params.extend(type_list)

    # WHERE 절 구성
    where_clause = "WHERE " + " AND ".join(filters) if filters else ""
    
    # 디버깅: 생성된 쿼리 정보 출력
    print(f"필터 개수: {len(filters)}")
    print(f"WHERE 절: {where_clause}")
    print(f"파라미터: {params}")
    print("==================")

    # 페이지네이션
    page = int(request.args.get("page", 1))
    per_page = 10
    offset = (page - 1) * per_page

    # 총 개수 조회 - 수정된 부분
    if filters:
        # 필터가 있는 경우, movie 테이블에서만 COUNT (EXISTS 조건들이 이미 필터링 처리)
        count_query = f"""
            SELECT COUNT(*) AS total
            FROM movie m
            {where_clause}
        """
    else:
        # 필터가 없는 경우 전체 영화 수
        count_query = "SELECT COUNT(*) AS total FROM movie"
    
    cur.execute(count_query, params)
    total_items = cur.fetchone()["total"]
    total_pages = (total_items + per_page - 1) // per_page
    
    print(f"총 개수: {total_items}")
    print("==================")

    # movie_id만 먼저 조회
    if filters:
        id_query = f"""
            SELECT m.movie_id 
            FROM movie m
            {where_clause}
            ORDER BY m.movie_id
            LIMIT %s OFFSET %s
        """
    else:
        id_query = f"""
            SELECT movie_id 
            FROM movie 
            ORDER BY movie_id
            LIMIT %s OFFSET %s
        """
    cur.execute(id_query, params + [per_page, offset])
    movie_ids = [row["movie_id"] for row in cur.fetchall()]

    if not movie_ids:
        close_db(conn, cur)
        return jsonify({
            "data": [],
            "pagination": {
                "total_pages": total_pages,
                "current_page": page,
                "total_items": total_items,
                "per_page": per_page
            }
        })

    # 해당 movie_id들의 상세 정보 조회
    placeholders = ",".join(["%s"] * len(movie_ids))
    detail_query = f"""
        SELECT 
            m.movie_id, m.title, m.english_title, m.open_year,
            m.type, m.status, m.company,
            GROUP_CONCAT(DISTINCT d.name ORDER BY d.name SEPARATOR ',') AS directors,
            GROUP_CONCAT(DISTINCT g.name ORDER BY g.name SEPARATOR ',') AS genres,
            GROUP_CONCAT(DISTINCT n.name ORDER BY n.name SEPARATOR ',') AS nations
        FROM movie m
        LEFT JOIN movie_director md ON m.movie_id = md.movie_id
        LEFT JOIN director d ON md.director_id = d.director_id
        LEFT JOIN movie_genre mg ON m.movie_id = mg.movie_id
        LEFT JOIN genre g ON mg.genre_id = g.genre_id
        LEFT JOIN movie_nation mn ON m.movie_id = mn.movie_id
        LEFT JOIN nation n ON mn.nation_id = n.nation_id
        WHERE m.movie_id IN ({placeholders})
        GROUP BY m.movie_id, m.title, m.english_title, m.open_year, m.type, m.status, m.company
        ORDER BY m.movie_id
    """
    cur.execute(detail_query, movie_ids)
    results = cur.fetchall()

    # 결과 처리
    for row in results:
        row["directors"] = [d.strip() for d in row["directors"].split(",")] if row["directors"] else []
        row["genres"] = [g.strip() for g in row["genres"].split(",")] if row["genres"] else []
        row["nations"] = [n.strip() for n in row["nations"].split(",")] if row["nations"] else []

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