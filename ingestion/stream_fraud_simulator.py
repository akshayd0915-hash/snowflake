import snowflake.connector
import random
import time
from datetime import datetime

# Connection
conn = snowflake.connector.connect(
    account   = "mec15672.us-east-1",
    user      = "AKSHAYDUBEY401",
    password  = os.environ.get("Iam90%dead10%evil"),
    warehouse = "COMPUTE_WH",
    database  = "BANKING_DW",
    schema    = "RAW",
    role      = "ACCOUNTADMIN"
)
cursor = conn.cursor()
print("Connected! Starting fraud simulation...\n")

# Simulate high-risk transactions in micro-batches
channels      = ["WIRE", "ONLINE", "MOBILE", "ATM", "BRANCH"]
categories    = ["TRANSFER", "PAYMENT", "WITHDRAWAL"]
high_risk_ids = ["CUST0001", "CUST0002", "CUST0003", "CUST0010", "CUST0020"]

batch_number = 1
try:
    while batch_number <= 5:  # Run 5 batches then stop
        print(f"Batch {batch_number} — inserting suspicious transactions...")

        batch = []
        for i in range(3):  # 3 transactions per batch
            txn_id = f"STREAM_TXN_{batch_number}_{i}_{random.randint(1000,9999)}"
            batch.append((
                txn_id,
                f"ACC{random.randint(1,350):05d}",
                random.choice(high_risk_ids),
                datetime.now().strftime("%Y-%m-%d"),
                round(random.uniform(5000, 15000), 2),  # High value amounts
                "TRANSFER",
                random.choice(["WIRE", "ONLINE"]),       # High risk channels
                "TRANSFER",
                f"Suspicious transaction {txn_id}",
                False
            ))

        cursor.executemany("""
            INSERT INTO BANKING_DW.RAW.TRANSACTIONS
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)

        print(f"  Inserted {len(batch)} transactions")

        # Check if stream captured them
        cursor.execute("""
            SELECT COUNT(*) 
            FROM BANKING_DW.RAW.TRANSACTIONS_STREAM
        """)
        stream_count = cursor.fetchone()[0]
        print(f"  Stream queue: {stream_count} rows waiting to be processed")
        print()

        batch_number += 1
        time.sleep(3)  # Wait 3 seconds between batches

except KeyboardInterrupt:
    print("\nSimulation stopped.")

finally:
    # Show final fraud alerts captured
    cursor.execute("""
        SELECT alert_reason, COUNT(*) as count, AVG(amount) as avg_amount
        FROM BANKING_DW.DBT_DEV_DBT_DEV.FRAUD_ALERTS_REALTIME
        GROUP BY alert_reason
        ORDER BY count DESC
    """)
    results = cursor.fetchall()
    if results:
        print("\nFraud alerts captured:")
        for row in results:
            print(f"  {row[0]}: {row[1]} alerts, avg amount ${row[2]:,.2f}")
    else:
        print("\nNo fraud alerts yet — task may still be processing")

    cursor.close()
    conn.close()
    print("\nDone!")