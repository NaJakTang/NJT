import pymysql
import random
import os
from dotenv import load_dotenv
from pymysql.cursors import DictCursor

# .env 파일 로드
load_dotenv()

try:
    # .env에서 비밀번호 불러오기
    db_password = os.getenv("DB_PASSWORD")
    if not db_password:
        raise ValueError(".env 파일에 DB_PASSWORD가 설정되지 않았습니다.")

    # MySQL 연결 설정
    conn = pymysql.connect(
        host='112.222.157.157',
        user='even_adv_sql_1',
        port=5298,
        password=db_password,
        database='db_even_adv_sql_1',
        cursorclass=DictCursor  # 딕셔너리 형태로 결과 받기
    )

    print("MySQL 연결 성공!")

    cursor = conn.cursor()

    def get_random_customer():
        cursor.execute("SELECT customer_id FROM customer ORDER BY RAND() LIMIT 1")
        return cursor.fetchone()

    def get_address_and_payment(customer_id):
        cursor.execute("""
            SELECT
                (SELECT address_id 
                FROM customer_address 
                WHERE customer_id = %s 
                ORDER BY RAND() 
                LIMIT 1) AS address_id,
                
                (SELECT cp.payment_id
                FROM customer_payment cp
                LEFT JOIN customer_credit cc ON cp.payment_id = cc.credit_id
                WHERE cp.customer_id = %s
                AND (
                        cc.credit_id IS NULL
                        OR (
                            cc.expire_date >= CURDATE()
                            AND cc.is_active = 1
                        )
                )
                ORDER BY RAND()
                LIMIT 1) AS payment_id
        """, (customer_id, customer_id))
        return cursor.fetchone()


    def insert_minimal_dummy_purchases():
        for _ in range(10000):  # 원하는 더미 데이터 수
            customer = get_random_customer()
            if not customer:
                continue
            customer_id = customer['customer_id']
            refs = get_address_and_payment(customer_id)
            if not refs['address_id'] or not refs['payment_id']:
                continue

            address_id = refs['address_id']
            payment_id = refs['payment_id']
            apply_ment = random.randint(1, 4)  # 1~4 중 하나

            sql = """
                INSERT INTO purchase (
                    customer_id, address_id, payment_id, apply_ment
                ) VALUES (%s, %s, %s, %s)
            """

            cursor.execute(sql, (customer_id, address_id, payment_id, apply_ment))

        conn.commit()
        print("✅ 구매 더미 데이터 삽입 완료.")

    insert_minimal_dummy_purchases()

except pymysql.MySQLError as e:
    print(f"❌ 연결 실패: {e}")

finally:
    if conn:
        cursor.close()
        conn.close()
        print("🔌 MySQL 연결 종료")
