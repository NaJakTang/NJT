import pymysql
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

try:
    # .env에서 비밀번호 및 기타 설정 불러오기
    db_password = os.getenv("DB_PASSWORD")
    if not db_password:
        raise ValueError(".env 파일에 DB_PASSWORD가 설정되지 않았습니다.")
    
    # MySQL 연결 설정
    connection = pymysql.connect(
        host='112.222.157.157',          # MySQL 서버 호스트
        user='even_adv_sql_1',     # MySQL 사용자 이름
        port=5298,                 # MySQL 포트
        password=db_password,      # MySQL 비밀번호
        database='db_even_adv_sql_1'               # 연결하려는 데이터베이스 이름
    )

    print("MySQL 연결 성공!")
       
        
except pymysql.MySQLError as e:
    print(f"연결 실패: {e}")

finally:
    if 'connection' in locals() and connection:  # is_connected() 대신 이 조건 사용
        connection.close()
        print("🔌 MySQL 연결 종료")