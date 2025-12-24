import psycopg2
import pandas as pd
from io import StringIO
import datetime

# PostgreSQL connection details
db_config = {
    "host": "192.168.160.194",
    "database": "verve",
    "user": "postgres",
    "password": "Sum#321",
    "port": "5433"
}

# Exact datetime range
START_DATETIME = "2025-12-21 00:00:00"
END_DATETIME   = "2025-12-23 23:59:59"

query = """
SELECT
    c.startdate                     AS "Start Date",
    c.enddate                       AS "End Date",
    u.name                          AS "Agent",
    camp.name                       AS "Campaign",
    c.phonenumber                   AS "Conference Number",
    c.accountcode                   AS "Account Code",
    TO_CHAR(make_interval(secs => c.confduration), 'HH24:MI:SS')
                                    AS "Conference Duration",
    c.customernumber                AS "Customer Number",
    c.hangupcause                   AS "Hangup Cause",
    c.confaccountcode               AS "Conference Account Code"
FROM cr_conf_log c
LEFT JOIN ct_user u        ON c.agentid = u.id
LEFT JOIN ct_campaign camp ON c.campid = camp.id
WHERE c.startdate BETWEEN %s AND %s
ORDER BY c.startdate;
"""

try:
    conn = psycopg2.connect(**db_config)

    df = pd.read_sql_query(
        query,
        conn,
        params=(START_DATETIME, END_DATETIME)
    )

    conn.close()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = (
        f"/tmp/conference_report_"
        f"2025-12-21to_23{current_time}.csv"
    )

    with open(file_path, "w") as f:
        f.write(csv_buffer.getvalue())

    print(f"✅ Conference Report generated successfully")
    print(f"📁 File Path: {file_path}")
    print(f"📊 Total Records: {len(df)}")

except Exception as e:
    print(f"❌ Error: {e}")
