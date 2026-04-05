# usage

# # CSV 出力（100件生成、出力件数上限100件）
# python orders_simulator.py --n 100 --max_records 100 --output csv

# # Kafka 出力（ブートストラップとトピック指定）
# python orders_simulator.py --n 1 --output kafka  --kafka_bootstrap "kafka1.local.data.platform:9093" --kafka_topic "access_log_topic" --kafka_user "admin" --kafka_password "admin"

# # Postgres 出力（接続文字列指定）
# python orders_simulator.py --n 100 --output postgres \
#   --pg_conn "dbname=mydb user=myuser password=mypass host=localhost"

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import pandas as pd
import uuid
import argparse
from datetime import datetime, timedelta
import time
import signal
import sys

from confluent_kafka import Producer
import psycopg2
import jump
import mmh3

P = 1    # partitions
B = 256    # logical buckets (固定)
bucket_assignment = [i % P for i in range(B)]  # 長さ256, 値は0..1

def simulate_orders_jp(n=100, group_rate=0.02, orphan_rate=0.01, include_test_errors=True):
    """
    日本語カラム名で Orders データを生成します。
    ID に UUID を使い重複リスクを排除。
    作成日時は「今日の 00:00:00〜23:59:59」の範囲でランダムに設定。
    
    Args:
        n: 生成する件数
        group_rate: 団体注文の比率
        orphan_rate: オーファンデータの比率
        include_test_errors: テスト用固定異常データを含めるかどうか
    """
    anomaly_count = max(1, int(n * group_rate))
    orphan_count = max(1, int(n * orphan_rate))
    normal_count = n - anomaly_count - orphan_count
    
    valid_user_ids = list(range(1, 101))
    valid_product_ids = list(range(1, 101))

    # 存在しないユーザーIDの候補を定義
    invalid_user_ids = [9999, 8888, 7777, 10001, 10002, 99999, 88888, 0, -1, 999999]

    now = datetime.now()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    def random_timestamp_today():
        sec = random.randint(0, 86399)
        return (start_of_day + timedelta(seconds=sec)).strftime('%Y-%m-%d %H:%M:%S')

    orders = []
    
    # 個人注文 (フラグ=1)
    for _ in range(normal_count):
        order_id = str(uuid.uuid4())
        user = random.choice(valid_user_ids)
        product = random.choice(valid_product_ids)
        price = round(random.uniform(100, 1000), 2)
        quantity = random.randint(1, 5)
        subtotal = round(price * quantity, 2)
        tax = round(subtotal * 0.1, 2)
        total = round(subtotal + tax, 2)
        created_at = random_timestamp_today()
        orders.append({
            'id': order_id,
            'user_id': user,
            'product_id': product,
            'subtotal_usd': subtotal,
            'tax_usd': tax,
            'total_usd': total,
            'quantity': quantity,
            'status_flag': 1,
            'created_at': created_at,
            'parent_id': None
        })

    # 団体注文 (フラグ=2) の手入力ミス
    for _ in range(anomaly_count):
        order_id = str(uuid.uuid4())
        user = random.choice(valid_user_ids)
        product = random.choice(valid_product_ids)
        price = round(random.uniform(100, 1000), 2)
        quantity = random.randint(10, 50)
        subtotal = round(price * quantity, 2)
        tax = round(subtotal * 0.1, 2)
        total = round(subtotal + tax + random.choice([-1, 1]), 2)
        created_at = random_timestamp_today()
        orders.append({
            'id': order_id,
            'user_id': user,
            'product_id': product,
            'subtotal_usd': subtotal,
            'tax_usd': tax,
            'total_usd': total,
            'quantity': quantity,
            'status_flag': 2,
            'created_at': created_at,
            'parent_id': None
        })

    # オーファン注文 (存在しないユーザーID)
    for _ in range(orphan_count):
        order_id = str(uuid.uuid4())
        product = random.choice(valid_product_ids)
        price = round(random.uniform(100, 1000), 2)
        quantity = random.randint(1, 5)
        subtotal = round(price * quantity, 2)
        tax = round(subtotal * 0.1, 2)
        total = round(subtotal + tax, 2)
        created_at = random_timestamp_today()
        orders.append({
            'id': order_id,
            'user_id': random.choice(invalid_user_ids), # 存在しないユーザーID
            'product_id': product,
            'subtotal_usd': subtotal,
            'tax_usd': tax,
            'total_usd': total,
            'quantity': quantity,
            'status_flag': 1,
            'created_at': created_at,
            'parent_id': None
        })

    # データ品質テスト用エラー行（オプション）
    if include_test_errors and len(orders) > 0:
        # 基本データをテンプレートとして使用
        base_order = {
            'id': str(uuid.uuid4()),
            'user_id': random.choice(valid_user_ids),
            'product_id': random.choice(valid_product_ids),
            'subtotal_usd': 1000.0,
            'tax_usd': 100.0,
            'total_usd': 1100.0,
            'quantity': 1,
            'status_flag': 2,
            'created_at': random_timestamp_today(),
            'parent_id': None
        }
        
        # 1. UTCタイムゾーンミス
        tz_error = base_order.copy()
        tz_error['id'] = str(uuid.uuid4())
        tz_error['created_at'] = '2025-05-02 06:00:00Z'
        orders.append(tz_error)
        
        # 2. 単位ミス
        unit_error = base_order.copy()
        unit_error['id'] = str(uuid.uuid4())
        unit_error['total_usd'] = 110000.0  # 円→セント単位ミス
        orders.append(unit_error)
        
        # 3. 小計欠損
        subtotal_error = base_order.copy()
        subtotal_error['id'] = str(uuid.uuid4())
        subtotal_error['subtotal_usd'] = None
        orders.append(subtotal_error)
        
        # 4. 税金額NULL（NaNではなくNone）
        tax_error = base_order.copy()
        tax_error['id'] = str(uuid.uuid4())
        tax_error['tax_usd'] = None
        orders.append(tax_error)
        
        # 5. 数量先頭ゼロ
        qty_error = base_order.copy()
        qty_error['id'] = str(uuid.uuid4())
        qty_error['quantity'] = '01'
        orders.append(qty_error)

    return pd.DataFrame(orders)


def main():
    parser = argparse.ArgumentParser(description='日本語Ordersシミュレーター with UUID & Today-timestamps')
    parser.add_argument('--n', type=int, default=100, help='生成する注文件数／出力件数の上限')
    parser.add_argument('--max_records', type=int, default=None, help='（任意）出力件数の上限。未指定は --n と同じ')
    parser.add_argument('--output', choices=['csv','kafka','postgres'], default='csv', help='出力先')
    parser.add_argument('--kafka_bootstrap', type=str, default='localhost:9092', help='Kafka ブートストラップサーバ')
    parser.add_argument('--kafka_topic', type=str, default='orders', help='Kafka トピック名')
    parser.add_argument('--kafka_user', type=str, help='Kafka SASL ユーザー名')
    parser.add_argument('--kafka_password', type=str, help='Kafka SASL パスワード')
    parser.add_argument('--continuous', action='store_true', help='継続送信モード（Ctrl+Cで停止）')
    parser.add_argument('--interval', type=float, default=1.0, help='継続モード時の送信間隔（秒）')
    parser.add_argument('--bad_data_rate', type=float, default=0.0, help='品質の悪いデータの比率（0.0-1.0）')
    parser.add_argument('--include_test_errors', action='store_true', help='テスト用固定異常データを含める')
    parser.add_argument('--pg_conn', type=str, 
                       default='host=host.docker.internal port=5435 dbname=domain_database user=domain password=domain',
                       help='Postgres 接続文字列')
    args = parser.parse_args()

    # bad_data_rateが0の場合はテスト用異常データも含めない
    include_test_errors = args.include_test_errors or (args.bad_data_rate > 0)
    
    df = simulate_orders_jp(n=args.n, include_test_errors=include_test_errors)
    limit = args.max_records if args.max_records is not None else args.n
    if len(df) > limit:
        df = df.sample(n=limit, random_state=1).reset_index(drop=True)

    if args.output == 'csv':
        df.to_csv('orders_jp.csv', index=False)
        print(f'CSV に {len(df)} 件出力しました')

    elif args.output == 'kafka':
        if not args.kafka_user or not args.kafka_password:
            print('Error: --kafka_user と --kafka_password を指定してください')
            return

        conf = {
            'bootstrap.servers':    args.kafka_bootstrap,
            'security.protocol':    'SASL_PLAINTEXT',
            'sasl.mechanisms':      'PLAIN',
            'sasl.username':        args.kafka_user,
            'sasl.password':        args.kafka_password,
            'message.send.max.retries':  3,
            'retry.backoff.ms':          500,
            'enable.idempotence':        'true',
            'max.in.flight.requests.per.connection': 5,
            'linger.ms':                 100,
            'batch.size':                65536,
            'compression.type':         'lz4',
            #'buffer.memory':            1048576,
            'queue.buffering.max.kbytes': 1024,
            'queue.buffering.max.messages': 100,
            'acks':                      'all',
            'delivery.timeout.ms':       12000,
        }
        producer = Producer(conf)
        send_error = False
        total_sent = 0

        def on_delivery(err, msg):
            nonlocal send_error
            if err is not None:
                print(f'[ERROR] Delivery failed for {msg.key()}: {err}')
                send_error = True

        def signal_handler(sig, frame):
            print(f'\n\n[INFO] 停止シグナルを受信しました。合計 {total_sent} 件送信しました。')
            producer.flush()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        if args.continuous:
            print(f'継続送信モードを開始します（間隔: {args.interval}秒, 品質悪化率:{args.bad_data_rate:.1%}）')
            print(f'送信先: {args.kafka_bootstrap} -> {args.kafka_topic}')
            
            try:
                while True:
                    # 各レコードを確率的に生成
                    target_count = args.max_records if args.max_records else args.n
                    normal_data = []
                    bad_data = []
                    
                    for i in range(target_count):
                        # 確率的に正常/異常を決定
                        is_bad = random.random() < args.bad_data_rate
                        
                        order_id = str(uuid.uuid4())
                        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        if is_bad:
                            # 異常データ生成
                            user = random.choice([9999, 8888, 7777, 0, -1])  # 無効ユーザー
                            product = random.choice(list(range(1, 101)))
                            price = round(random.uniform(100, 1000), 2)
                            quantity = random.randint(10, 50)
                            subtotal = round(price * quantity, 2)
                            tax = float('nan')  # 異常パターン
                            total = round(subtotal, 2)
                            
                            bad_data.append({
                                'id': order_id,
                                'user_id': user,
                                'product_id': product,
                                'subtotal_usd': subtotal,
                                'tax_usd': tax,
                                'total_usd': total,
                                'quantity': quantity,
                                'flag': 2,
                                'created_at': created_at
                            })
                        else:
                            # 正常データ生成
                            user = random.choice(list(range(1, 101)))
                            product = random.choice(list(range(1, 101)))
                            price = round(random.uniform(100, 1000), 2)
                            quantity = random.randint(1, 5)
                            subtotal = round(price * quantity, 2)
                            tax = round(subtotal * 0.1, 2)
                            total = round(subtotal + tax, 2)

                            print(f'[DEBUG] 正常データ生成 送信日時:{datetime.now()}-----> {order_id}, ユーザーID: {user}, 製品ID: {product}, 小計: {subtotal}, 税金: {tax}, 合計: {total}, 数量: {quantity}')
                            
                            normal_data.append({
                                'id': order_id,
                                'user_id': user,
                                'product_id': product,
                                'subtotal_usd': subtotal,
                                'tax_usd': tax,
                                'total_usd': total,
                                'quantity': quantity,
                                'flag': 1,
                                'created_at': created_at
                            })
                    
                    # 結合してシャッフル
                    all_data = normal_data + bad_data
                    random.shuffle(all_data)
                    fresh_df = pd.DataFrame(all_data)

                    # 送信処理
                    for _, row in fresh_df.iterrows():

                        b = jump.hash(mmh3.hash64(row['id'])[0], B)  # 論理バケットID
                        partition = bucket_assignment[b]          # 物理パーティションID

                        # テスト用に特定ユーザーIDで固定
                        #row['user_id'] = 98

                        producer.produce(
                            args.kafka_topic,
                            key=row['id'],
                            value=row.to_json(force_ascii=False).encode('utf-8'),
                            partition=partition,
                            callback=on_delivery
                        )
                        producer.poll(0)
                        if send_error:
                            break
                    
                    if send_error:
                        break
                        
                    total_sent += len(fresh_df)
                    print(f'[INFO] {len(fresh_df)} 件送信 (正常:{len(normal_data)}, 異常:{len(bad_data)},  累計:{total_sent})')
                    time.sleep(args.interval)
                    
            except KeyboardInterrupt:
                print(f'\n[INFO] 停止しました。合計 {total_sent} 件送信しました。')
            finally:
                producer.flush()
        else:
            # 従来の1回のみ送信モード
            for _, row in df.iterrows():
                producer.produce(
                    args.kafka_topic,
                    key=row['id'],
                    value=row.to_json(force_ascii=False).encode('utf-8'),
                    callback=on_delivery
                )
                producer.poll(0)
                if send_error:
                    print('送信失敗が発生したため処理を中断します。')
                    break

            producer.flush()
            if not send_error:
                print(f'Kafka に {len(df)} 件送信完了しました')

    elif args.output == 'postgres':
        conn = None
        total_inserted = 0
        
        def signal_handler(sig, frame):
            print(f'\n\n[INFO] 停止シグナルを受信しました。合計 {total_inserted} 件挿入しました。')
            if conn:
                conn.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        try:
            conn = psycopg2.connect(args.pg_conn)
            print(f'PostgreSQL接続成功: {args.pg_conn}')

            if args.continuous:
                print(f'PostgreSQL継続挿入モードを開始します（間隔: {args.interval}秒, 品質悪化率:{args.bad_data_rate:.1%}）')
                print('挿入先: orders テーブル')
                
                try:
                    while True:
                        # 各レコードを確率的に生成
                        target_count = args.max_records if args.max_records else args.n
                        normal_data = []
                        bad_data = []
                        
                        for i in range(target_count):
                            # 確率的に正常/異常を決定
                            is_bad = random.random() < args.bad_data_rate
                            
                            order_id = str(uuid.uuid4())
                            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            if is_bad:
                                # 異常データ生成（様々な異常パターン）
                                user = random.choice([9999, 8888, 7777, 0, -1])  # 無効ユーザー
                                product = random.choice(list(range(1, 101)))
                                price = round(random.uniform(100, 1000), 2)
                                quantity = random.randint(10, 50)
                                subtotal = round(price * quantity, 2)
                                
                                # 異常パターンをランダムに選択
                                anomaly_type = random.choice(['null_tax', 'wrong_total', 'null_subtotal', 'zero_qty'])
                                
                                if anomaly_type == 'null_tax':
                                    tax = None  # 税金額がNULL
                                    total = round(subtotal, 2)
                                elif anomaly_type == 'wrong_total':
                                    tax = round(subtotal * 0.1, 2)
                                    total = round(subtotal + tax + random.choice([-100, 100]), 2)  # 合計が間違い
                                elif anomaly_type == 'null_subtotal':
                                    subtotal = None  # 小計がNULL
                                    tax = round(price * quantity * 0.1, 2)
                                    total = round(price * quantity + tax, 2)
                                else:  # zero_qty
                                    tax = round(subtotal * 0.1, 2)
                                    total = round(subtotal + tax, 2)
                                    quantity = 0  # 数量が0
                                
                                bad_data.append({
                                    'id': order_id,
                                    'user_id': user,
                                    'product_id': product,
                                    'subtotal_usd': subtotal,
                                    'tax_usd': tax,
                                    'total_usd': total,
                                    'quantity': quantity,
                                    'flag': 2,
                                    'created_at': created_at,
                                    'parent_id': None
                                })
                            else:
                                # 正常データ生成
                                user = random.choice(list(range(1, 101)))
                                product = random.choice(list(range(1, 101)))
                                price = round(random.uniform(100, 1000), 2)
                                quantity = random.randint(1, 5)
                                subtotal = round(price * quantity, 2)
                                tax = round(subtotal * 0.1, 2)
                                total = round(subtotal + tax, 2)

                                print(f'[DEBUG] 正常データ生成 挿入日時:{datetime.now()}-----> {order_id}, ユーザーID: {user}, 製品ID: {product}, 小計: {subtotal}, 税金: {tax}, 合計: {total}, 数量: {quantity}')
                                
                                normal_data.append({
                                    'id': order_id,
                                    'user_id': user,
                                    'product_id': product,
                                    'subtotal_usd': subtotal,
                                    'tax_usd': tax,
                                    'total_usd': total,
                                    'quantity': quantity,
                                    'flag': 1,
                                    'created_at': created_at,
                                    'parent_id': None
                                })
                        
                        # 結合してシャッフル
                        all_data = normal_data + bad_data
                        random.shuffle(all_data)
                        fresh_df = pd.DataFrame(all_data)
                        
                        # PostgreSQLに挿入
                        cur = conn.cursor()
                        insert_sql = '''
                        INSERT INTO orders
                        ("id", "user_id", "product_id", "subtotal_usd", "tax_usd", "total_usd", "quantity", "flag", "creation_date", "parent_id")
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        '''
                        
                        batch_inserted = 0
                        for _, row in fresh_df.iterrows():
                            try:
                                # None値をそのまま渡す（PostgreSQLでNULLになる）
                                cur.execute(insert_sql, (
                                    row['id'], 
                                    row['user_id'], 
                                    row['product_id'], 
                                    row['subtotal_usd'] if pd.notna(row['subtotal_usd']) else None,
                                    row['tax_usd'] if pd.notna(row['tax_usd']) else None,
                                    row['total_usd'] if pd.notna(row['total_usd']) else None,
                                    row['quantity'], 
                                    row['flag'], 
                                    row['created_at'], 
                                    row['parent_id']
                                ))
                                batch_inserted += 1
                            except psycopg2.Error as e:
                                print(f'[ERROR] 挿入エラー (ID: {row["id"]}): {e}')
                                conn.rollback()
                                cur = conn.cursor()  # カーソルを再作成
                        
                        conn.commit()
                        cur.close()
                        
                        total_inserted += batch_inserted
                        print(f'[INFO] {batch_inserted} 件挿入 (正常:{len(normal_data)}, 異常:{len(bad_data)}, 累計:{total_inserted})')
                        time.sleep(args.interval)
                        
                except KeyboardInterrupt:
                    print(f'\n[INFO] 停止しました。合計 {total_inserted} 件挿入しました。')
                finally:
                    if conn:
                        conn.close()
            else:
                # 従来の1回のみ挿入モード（修正版）
                df = simulate_orders_jp(n=args.n, include_test_errors=include_test_errors)  # ← パラメータを追加
                limit = args.max_records if args.max_records is not None else args.n
                if len(df) > limit:
                    df = df.sample(n=limit, random_state=1).reset_index(drop=True)
                
                print(f'PostgreSQLに {len(df)} 件挿入します')
                print(f'テスト用異常データ含む: {include_test_errors}')
                print(f'bad_data_rate: {args.bad_data_rate}')
                
                # デバッグ用: DataFrame内容確認
                print(f'[DEBUG] 挿入予定データ:')
                for idx, row in df.iterrows():
                    print(f'  行{idx}: ID={row["id"]}, 製品ID={row["product_id"]}, ユーザーID={row["user_id"]}, フラグ={row["flag"]}, 税金額={row["tax_usd"]}, 作成日時={row["created_at"]}, 小計={row["subtotal_usd"]}, 数量={row["quantity"]}')

                cur = conn.cursor()
                insert_sql = '''
                INSERT INTO orders
                ("id", "user_id", "product_id", "subtotal_usd", "tax_usd", "total_usd", "quantity", "flag", "creation_date", "parent_id")
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''
                
                for _, row in df.iterrows():
                    cur.execute(insert_sql, (
                        row['id'], 
                        row['user_id'], 
                        row['product_id'], 
                        row['subtotal_usd'] if pd.notna(row['subtotal_usd']) else None,
                        row['tax_usd'] if pd.notna(row['tax_usd']) else None,
                        row['total_usd'] if pd.notna(row['total_usd']) else None,
                        row['quantity'], 
                        row['flag'], 
                        row['created_at'], 
                        row['parent_id']
                    ))
                conn.commit()
                cur.close()
                print(f'ordersテーブルに {len(df)} 件登録しました')
                
        except Exception as e:
            print(f'Postgres接続エラー: {e}')
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    main()
