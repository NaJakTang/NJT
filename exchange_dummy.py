import pymysql
import random
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymysql.cursors import DictCursor

# .env 파일 로드
load_dotenv()

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
        cursorclass=DictCursor,
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    # 교환 가능한 purchase_item 조회 (item_status = 1, refund에 없는, exchange에도 없는)
    cursor.execute("""
        SELECT pi.purchase_item_id, pi.customer_id, pi.product_detail_id, pd.product_id, pd.sale_price
        FROM purchase_item pi
        JOIN product_detail pd ON pi.product_detail_id = pd.product_detail_id
        LEFT JOIN refund r ON pi.purchase_item_id = r.purchase_item_id
        WHERE pi.item_status = 1
        AND r.purchase_item_id IS NULL
        AND pi.purchase_item_id NOT IN (SELECT purchase_item_id FROM exchange)
        LIMIT 10
    """)
    purchase_items = cursor.fetchall()

    # customer_payment 목록
    cursor.execute("SELECT payment_id, customer_id FROM customer_payment")
    payment_ids = cursor.fetchall()

    inserted_count = 0
    for item in purchase_items:
        purchase_item_id = item['purchase_item_id']
        customer_id = item['customer_id']
        orig_product_detail_id = item['product_detail_id']
        orig_product_id = item['product_id']
        orig_sale_price = item['sale_price']

        # 동일 product_id 내 product_detail_id 조회 (원래 product_detail_id 포함)
        cursor.execute("""
            SELECT product_detail_id, sale_price
            FROM product_detail
            WHERE product_id = %s
        """, (orig_product_id,))
        exchange_products = cursor.fetchall()

        if not exchange_products:
            print(f"product_id {orig_product_id}에 대한 교환 가능한 product_detail 없음")
            continue

        exchange_product = random.choice(exchange_products)
        product_detail_id = exchange_product['product_detail_id']
        exchange_sale_price = exchange_product['sale_price']
        price_difference = round(exchange_sale_price - orig_sale_price, 2)

        # payment_id 결정
        payment_id = None
        if price_difference != 0:
            valid_payments = [p['payment_id'] for p in payment_ids if p['customer_id'] == customer_id]
            if valid_payments:
                payment_id = random.choice(valid_payments)
            else:
                print(f"customer_id {customer_id}에 대한 payment_id 없음")
                continue

        # 기존 교환 요청이 있는 경우 ex_exchange_id 설정
        cursor.execute("""
            SELECT exchange_id FROM exchange
            WHERE purchase_item_id = %s
            ORDER BY created_at DESC LIMIT 1
        """, (purchase_item_id,))
        prev_exchange = cursor.fetchone()
        ex_exchange_id = prev_exchange['exchange_id'] if prev_exchange else None

        # 교환 사유 및 상세 사유
        reason_map = {
            '단순변심': "다른상품으로 교환하고 싶어요",
            '배송문제': "상품이 파손되었어요",
            '상품문제': "다른상품이 배송되었어요"
        }
        exchange_reason = random.choice(list(reason_map.keys()))
        reason_detail = reason_map[exchange_reason]

        # 기타 필드
        status = 0
        delivery_due_date = (datetime.now() + timedelta(days=random.randint(3, 14))).strftime('%Y-%m-%d')
        created_at = datetime.now().strftime('%Y-%m-%d')
        updated_at = None

        # INSERT
        cursor.execute("""
            INSERT INTO `exchange` (
                purchase_item_id, product_detail_id, customer_id, payment_id,
                ex_exchange_id, price_difference, exchange_reason, reason_detail,
                status, delivery_due_date, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            purchase_item_id, product_detail_id, customer_id, payment_id,
            ex_exchange_id, price_difference, exchange_reason, reason_detail,
            status, delivery_due_date, created_at, updated_at
        ))

        inserted_count += 1

    conn.commit()
    print(f"{inserted_count}개의 교환 데이터가 삽입되었습니다.")

except Exception as e:
    print(f"오류 발생: {e}")
    if conn:
        conn.rollback()

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

print("교환 데이터 삽입 완료!")
