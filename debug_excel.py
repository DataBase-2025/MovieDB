import pandas as pd

file_path = "KOBIS_movie_info.xlsx"

# 시트 0: 컬럼 포함
df1 = pd.read_excel(file_path, sheet_name=0)

# 시트 1: 컬럼 없음
df2 = pd.read_excel(file_path, sheet_name=1, header=None)
df2.columns = ['영화명', '영화명(영문)', '제작연도', '제작국가', '유형', '장르', '제작상태', '감독', '제작사']

# 각 시트 정보 출력
print("📄 시트1 행 수:", df1.shape[0])
print("📄 시트2 행 수:", df2.shape[0])

print("🧩 시트1 컬럼:", df1.columns.tolist())

# 시트1에서 영화명이 있는 행 수
if '영화명' in df1.columns:
    print("🎬 시트1 유효한 영화명 행 수:", df1['영화명'].notna().sum())
else:
    print("❌ 시트1에 '영화명' 컬럼이 존재하지 않음!")

# 병합 후 확인
df = pd.concat([df1, df2], ignore_index=True)
print("🔀 병합된 총 행 수:", df.shape[0])

# 병합 후 유효한 영화명 수 확인
print("✅ 병합된 유효한 영화명 수:", df.dropna(subset=['영화명']).shape[0])
