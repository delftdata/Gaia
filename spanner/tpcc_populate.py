import os
import random
import string
import uuid
import time
import datetime
import concurrent.futures
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials

# TPC-C spec constants
ITEMS_COUNT = 100000
DISTRICTS_PER_WAREHOUSE = 10
CUSTOMERS_PER_DISTRICT = 3000

def random_astring(min_len, max_len):
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_nstring(min_len, max_len):
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.digits, k=length))

def make_c_last(num):
    syllables = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]
    return syllables[(num // 100) % 10] + syllables[(num // 10) % 10] + syllables[num % 10]

def NURand(A, x, y):
    # Simplified NURand for data loading
    C = random.randint(0, A)
    return (((random.randint(0, A) | random.randint(x, y)) + C) % (y - x + 1)) + x

def populate_items(database):
    print("Populating items (100k)...")
    columns = ("i_id", "i_im_id", "i_name", "i_price", "i_data")
    batch_size = 3000
    for offset in range(0, ITEMS_COUNT, batch_size):
        with database.batch() as batch:
            values = []
            for i in range(offset, min(offset + batch_size, ITEMS_COUNT)):
                i_id = i + 1
                i_im_id = random.randint(1, 10000)
                i_name = random_astring(14, 24)
                i_price = random.uniform(1.0, 100.0)
                i_data = random_astring(26, 50)
                if random.random() < 0.1:
                    # Insert "ORIGINAL" randomly
                    pos = random.randint(0, len(i_data) - 8)
                    i_data = i_data[:pos] + "ORIGINAL" + i_data[pos+8:]
                values.append((i_id, i_im_id, i_name, i_price, i_data))
            batch.insert_or_update(table="item", columns=columns, values=values)
    print("Items populated.")

def populate_warehouse_and_districts(w_id, database):
    # 1 Warehouse
    w_name = random_astring(6, 10)
    w_street_1 = random_astring(10, 20)
    w_street_2 = random_astring(10, 20)
    w_city = random_astring(10, 20)
    w_state = random_astring(2, 2)
    w_zip = random_astring(9, 9)
    w_tax = random.uniform(0, 0.2)
    w_ytd = 300000.0

    with database.batch() as batch:
        batch.insert_or_update(
            table="warehouse",
            columns=("w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"),
            values=[(w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)]
        )

    # 10 Districts
    columns = ("d_w_id", "d_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id")
    with database.batch() as batch:
        values = []
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            d_name = random_astring(6, 10)
            d_street_1 = random_astring(10, 20)
            d_street_2 = random_astring(10, 20)
            d_city = random_astring(10, 20)
            d_state = random_astring(2, 2)
            d_zip = random_astring(9, 9)
            d_tax = random.uniform(0, 0.2)
            d_ytd = 30000.0
            d_next_o_id = 3001
            values.append((w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id))
        batch.insert_or_update(table="district", columns=columns, values=values)

def populate_stock_for_warehouse(w_id, database):
    columns = ("s_w_id", "s_i_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data")
    batch_size = 1000
    for offset in range(0, ITEMS_COUNT, batch_size):
        with database.batch() as batch:
            values = []
            for i in range(offset, min(offset + batch_size, ITEMS_COUNT)):
                s_i_id = i + 1
                s_quantity = random.randint(10, 100)
                dists = [random_astring(24, 24) for _ in range(10)]
                s_ytd = 0
                s_order_cnt = 0
                s_remote_cnt = 0
                s_data = random_astring(26, 50)
                if random.random() < 0.1:
                    pos = random.randint(0, len(s_data) - 8)
                    s_data = s_data[:pos] + "ORIGINAL" + s_data[pos+8:]
                
                values.append((w_id, s_i_id, s_quantity, *dists, s_ytd, s_order_cnt, s_remote_cnt, s_data))
            batch.insert_or_update(table="stock", columns=columns, values=values)

def populate_customers_for_district(w_id, d_id, database):
    c_cols = ("c_w_id", "c_d_id", "c_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data")
    h_cols = ("h_uuid", "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data")
    
    batch_size = 400 # 21 cols + 9 cols = 30 cols, 400 * 30 = 12000 cells
    for offset in range(0, CUSTOMERS_PER_DISTRICT, batch_size):
        with database.batch() as batch:
            c_values = []
            h_values = []
            for c in range(offset, min(offset + batch_size, CUSTOMERS_PER_DISTRICT)):
                c_id = c + 1
                c_first = random_astring(8, 16)
                c_middle = "OE"
                if c_id <= 1000:
                    c_last = make_c_last(c_id - 1)
                else:
                    c_last = make_c_last(NURand(255, 0, 999))
                
                c_street_1 = random_astring(10, 20)
                c_street_2 = random_astring(10, 20)
                c_city = random_astring(10, 20)
                c_state = random_astring(2, 2)
                c_zip = random_astring(9, 9)
                c_phone = random_nstring(16, 16)
                c_since = spanner.COMMIT_TIMESTAMP
                c_credit = "BC" if random.random() < 0.1 else "GC"
                c_credit_lim = 50000.0
                c_discount = random.uniform(0, 0.5)
                c_balance = -10.0
                c_ytd_payment = 10.0
                c_payment_cnt = 1
                c_delivery_cnt = 0
                c_data = random_astring(300, 500)
                
                c_values.append((w_id, d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data))
                
                # History
                h_uuid = str(uuid.uuid4())
                h_amount = 10.0
                h_data = random_astring(12, 24)
                h_values.append((h_uuid, c_id, d_id, w_id, d_id, w_id, spanner.COMMIT_TIMESTAMP, h_amount, h_data))
                
            batch.insert_or_update(table="customer", columns=c_cols, values=c_values)
            batch.insert_or_update(table="history", columns=h_cols, values=h_values)

def populate_orders_for_district(w_id, d_id, database):
    o_cols = ("o_w_id", "o_d_id", "o_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local")
    no_cols = ("no_w_id", "no_d_id", "no_o_id")
    ol_cols = ("ol_w_id", "ol_d_id", "ol_o_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info")
    
    # 3000 orders
    c_ids = list(range(1, 3001))
    random.shuffle(c_ids)
    
    # 100 orders per batch to avoid 20k cell limits (max 16100 mutations)
    batch_size = 100
    for offset in range(0, 3000, batch_size):
        with database.batch() as batch:
            o_vals = []
            no_vals = []
            ol_vals = []
            for i in range(offset, min(offset + batch_size, 3000)):
                o_id = i + 1
                o_c_id = c_ids[i]
                o_carrier_id = random.randint(1, 10) if o_id < 2101 else None
                o_ol_cnt = random.randint(5, 15)
                o_all_local = 1
                
                o_vals.append((w_id, d_id, o_id, o_c_id, spanner.COMMIT_TIMESTAMP, o_carrier_id, o_ol_cnt, o_all_local))
                
                if o_id >= 2101:
                    no_vals.append((w_id, d_id, o_id))
                    
                for ol_num in range(1, o_ol_cnt + 1):
                    ol_i_id = random.randint(1, ITEMS_COUNT)
                    ol_supply_w_id = w_id
                    ol_delivery_d = spanner.COMMIT_TIMESTAMP if o_id < 2101 else None
                    ol_quantity = 5
                    ol_amount = 0.0 if o_id < 2101 else random.uniform(0.01, 9999.99)
                    ol_dist_info = random_astring(24, 24)
                    
                    ol_vals.append((w_id, d_id, o_id, ol_num, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info))
            
            batch.insert_or_update(table="orders", columns=o_cols, values=o_vals)
            if no_vals:
                batch.insert_or_update(table="new_order", columns=no_cols, values=no_vals)
            batch.insert_or_update(table="order_line", columns=ol_cols, values=ol_vals)

def worker_populate_warehouse(w_id, primary_ip, project, instance, db_name, port):
    os.environ["SPANNER_EMULATOR_HOST"] = f"{primary_ip}:{port}"
    client = spanner.Client(project=project, credentials=AnonymousCredentials())
    inst = client.instance(instance)
    database = inst.database(db_name)
    
    start_time = time.time()
    try:
        populate_warehouse_and_districts(w_id, database)
        populate_stock_for_warehouse(w_id, database)
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            populate_customers_for_district(w_id, d_id, database)
            populate_orders_for_district(w_id, d_id, database)
        print(f"Warehouse {w_id} populated in {time.time()-start_time:.1f}s")
    except Exception as e:
        print(f"Error populating warehouse {w_id}: {e}")

def create_tpcc_schema(database):
    print("Creating TPC-C Schema on Spanner...")
    ddl = [
        "CREATE TABLE warehouse ( w_id INT64 NOT NULL, w_name STRING(10), w_street_1 STRING(20), w_street_2 STRING(20), w_city STRING(20), w_state STRING(2), w_zip STRING(9), w_tax FLOAT64, w_ytd FLOAT64 ) PRIMARY KEY (w_id)",
        "CREATE TABLE district ( d_w_id INT64 NOT NULL, d_id INT64 NOT NULL, d_name STRING(10), d_street_1 STRING(20), d_street_2 STRING(20), d_city STRING(20), d_state STRING(2), d_zip STRING(9), d_tax FLOAT64, d_ytd FLOAT64, d_next_o_id INT64 ) PRIMARY KEY (d_w_id, d_id)",
        "CREATE TABLE customer ( c_w_id INT64 NOT NULL, c_d_id INT64 NOT NULL, c_id INT64 NOT NULL, c_first STRING(16), c_middle STRING(2), c_last STRING(16), c_street_1 STRING(20), c_street_2 STRING(20), c_city STRING(20), c_state STRING(2), c_zip STRING(9), c_phone STRING(16), c_since TIMESTAMP OPTIONS (allow_commit_timestamp=true), c_credit STRING(2), c_credit_lim FLOAT64, c_discount FLOAT64, c_balance FLOAT64, c_ytd_payment FLOAT64, c_payment_cnt INT64, c_delivery_cnt INT64, c_data STRING(500) ) PRIMARY KEY (c_w_id, c_d_id, c_id)",
        "CREATE TABLE history ( h_uuid STRING(36) NOT NULL, h_c_id INT64, h_c_d_id INT64, h_c_w_id INT64, h_d_id INT64, h_w_id INT64, h_date TIMESTAMP OPTIONS (allow_commit_timestamp=true), h_amount FLOAT64, h_data STRING(24) ) PRIMARY KEY (h_uuid)",
        "CREATE TABLE new_order ( no_w_id INT64 NOT NULL, no_d_id INT64 NOT NULL, no_o_id INT64 NOT NULL ) PRIMARY KEY (no_w_id, no_d_id, no_o_id)",
        "CREATE TABLE orders ( o_w_id INT64 NOT NULL, o_d_id INT64 NOT NULL, o_id INT64 NOT NULL, o_c_id INT64, o_entry_d TIMESTAMP OPTIONS (allow_commit_timestamp=true), o_carrier_id INT64, o_ol_cnt INT64, o_all_local INT64 ) PRIMARY KEY (o_w_id, o_d_id, o_id)",
        "CREATE TABLE order_line ( ol_w_id INT64 NOT NULL, ol_d_id INT64 NOT NULL, ol_o_id INT64 NOT NULL, ol_number INT64 NOT NULL, ol_i_id INT64, ol_supply_w_id INT64, ol_delivery_d TIMESTAMP OPTIONS (allow_commit_timestamp=true), ol_quantity INT64, ol_amount FLOAT64, ol_dist_info STRING(24) ) PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)",
        "CREATE TABLE item ( i_id INT64 NOT NULL, i_im_id INT64, i_name STRING(24), i_price FLOAT64, i_data STRING(50) ) PRIMARY KEY (i_id)",
        "CREATE TABLE stock ( s_w_id INT64 NOT NULL, s_i_id INT64 NOT NULL, s_quantity INT64, s_dist_01 STRING(24), s_dist_02 STRING(24), s_dist_03 STRING(24), s_dist_04 STRING(24), s_dist_05 STRING(24), s_dist_06 STRING(24), s_dist_07 STRING(24), s_dist_08 STRING(24), s_dist_09 STRING(24), s_dist_10 STRING(24), s_ytd INT64, s_order_cnt INT64, s_remote_cnt INT64, s_data STRING(50) ) PRIMARY KEY (s_w_id, s_i_id)",
        "CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
        "CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id, o_id DESC)"
    ]
    
    try:
        operation = database.update_ddl(ddl)
        operation.result(300)
        print("TPC-C Schema created successfully.")
    except Exception as e:
        print(f"TPC-C Schema update message (may already exist): {e}")

def run_tpcc_population(primary_ip, warehouse_count, project, instance, db_name, port):
    print(f"--- Populating TPC-C: {warehouse_count} warehouses ---")
    os.environ["SPANNER_EMULATOR_HOST"] = f"{primary_ip}:{port}"
    client = spanner.Client(project=project, credentials=AnonymousCredentials())
    inst = client.instance(instance)
    database = inst.database(db_name)
    
    create_tpcc_schema(database)
    populate_items(database)
    
    # We use ThreadPoolExecutor to populate warehouses in parallel. 
    max_workers = min(16, warehouse_count)
    print(f"Starting {max_workers} worker threads for warehouse population...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for w_id in range(1, warehouse_count + 1):
            futures.append(executor.submit(worker_populate_warehouse, w_id, primary_ip, project, instance, db_name, port))
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                future.result()
            except Exception as e:
                print(f"Warehouse population failed: {e}")
            
    print("--- TPC-C Population Complete ---")
