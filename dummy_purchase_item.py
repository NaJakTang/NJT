import pymysql
import random
import os
from dotenv import load_dotenv
from pymysql.cursors import DictCursor

# .env 파일 로드
load_dotenv()

# DB 연결
conn = None
cursor = None

try:
    db_password = os.getenv("DB_PASSWORD")
    if not db_password:
        raise ValueError(".env 파일에 DB_PASSWORD가 없습니다.")

    conn = pymysql.connect(
        host='112.222.157.157',
        user='even_adv_sql_1',
        port=5298,
        password=db_password,
        database='db_even_adv_sql_1',
        cursorclass=DictCursor
    )
    cursor = conn.cursor()

    # 연결 상태 확인
    if conn.open:
        print("MySQL 연결 성공!")

    # 63~80 purchase_id 가져오기
    cursor.execute("""
        SELECT purchase_id 
        FROM purchase 
        WHERE purchase_id BETWEEN 63 AND 80
    """)
    purchase_data = cursor.fetchall()

    # product_detail과 product, business 연결 정보 가져오기
    cursor.execute("""
        SELECT pd.product_detail_id, pd.product_id, p.business_id
        FROM product_detail pd
        JOIN product p ON pd.product_id = p.product_id
    """)
    product_detail_map = cursor.fetchall()

    # business_id → product_id 집합
    business_to_product_ids = {}
    product_id_to_detail_ids = {}

    for row in product_detail_map:
        pd_id = row["product_detail_id"]
        product_id = row["product_id"]
        business_id = row["business_id"]
        business_to_product_ids.setdefault(business_id, set()).add(product_id)
        product_id_to_detail_ids.setdefault(product_id, []).append(pd_id)

    # INSERT용 쿼리
    insert_query = """
    INSERT INTO purchase_item (purchase_id, product_detail_id, quantity)
    VALUES (%s, %s, %s)
    """

    dummy_data = []

    for purchase in purchase_data:
        purchase_id = purchase["purchase_id"]
        for _ in range(random.randint(2, 5)):  # 하나의 구매에 2~5개의 아이템
            use_same_business = random.random() < 0.5

            if use_same_business:
                # 동일한 business_id의 product_id 선택
                base_row = random.choice(product_detail_map)
                base_business_id = base_row["business_id"]
                base_pd_id = base_row["product_detail_id"]
                possible_product_ids = list(business_to_product_ids.get(base_business_id, []))
                if not possible_product_ids:
                    continue  # 가능한 product_id가 없으면 스킵
                chosen_product_id = random.choice(possible_product_ids)
                chosen_detail_ids = product_id_to_detail_ids.get(chosen_product_id, [base_pd_id])
                chosen_pd_id = random.choice(chosen_detail_ids)
            else:
                # 임의의 product_detail_id 선택
                chosen_row = random.choice(product_detail_map)
                chosen_pd_id = chosen_row["product_detail_id"]

            quantity = round(random.uniform(1, 5), 2)
            dummy_data.append((purchase_id, chosen_pd_id, quantity))

    # INSERT 실행
    if dummy_data:
        cursor.executemany(insert_query, dummy_data)
        conn.commit()
        print(f"삽입한 데이터: {dummy_data}")
        print(f"{cursor.rowcount} rows inserted into purchase_item.")
    else:
        print("삽입할 데이터가 없습니다.")

except pymysql.MySQLError as e:
    print(f"MySQL 에러 발생: {e}")
except ValueError as e:
    print(f"값 에러 발생: {e}")
except Exception as e:
    print(f"기타 에러 발생: {e}")
finally:
    if cursor:
        cursor.close()
    if conn and conn.open:
        conn.close()
        print("MySQL 연결 종료")