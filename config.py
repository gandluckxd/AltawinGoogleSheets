import os
from dotenv import load_dotenv

load_dotenv()

# Настройки подключения к базе данных Altawin
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.8.0.3'),
    'port': int(os.getenv('DB_PORT', '3050')),
    'database': os.getenv('DB_DATABASE', 'D:/altAwinDB/ppk.gdb'),
    'user': os.getenv('DB_USER', 'sysdba'),
    'password': os.getenv('DB_PASSWORD', 'masterkey'),
    'charset': os.getenv('DB_CHARSET', 'WIN1251')
}

# Настройки Google Sheets
GOOGLE_SHEETS_CONFIG = {
    'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json'),
    'spreadsheet_name': os.getenv('GOOGLE_SPREADSHEET_NAME', 'FMO Altawin'),
    'worksheet_name': os.getenv('GOOGLE_WORKSHEET_NAME', 'Лист1')
}

# SQL-запрос
# Используем параметры ? для подстановки дат
SQL_QUERY = """
WITH 
izd_pvh AS (
    SELECT
        o.proddate,
        SUM(oi.qty) AS qty1
    FROM orders o
    JOIN orderitems oi ON oi.orderid = o.orderid
    JOIN models m ON m.orderitemsid = oi.orderitemsid
    JOIN r_systems rs ON rs.rsystemid = m.sysprofid
    WHERE o.proddate BETWEEN ? AND ?
        AND rs.systemtype = 0
        AND rs.rsystemid <> 8
    GROUP BY o.proddate
),
razdv AS (
    SELECT
        o.proddate,
        SUM(oi.qty) AS qty2
    FROM orders o
    JOIN orderitems oi ON oi.orderid = o.orderid
    JOIN models m ON m.orderitemsid = oi.orderitemsid
    JOIN r_systems rs ON rs.rsystemid = m.sysprofid
    WHERE o.proddate BETWEEN ? AND ?
        AND ((rs.systemtype = 1) OR (rs.rsystemid = 8))
    GROUP BY o.proddate
),
mosnet AS (
    SELECT
        o.proddate,
        SUM(oi.qty * itd.qty) AS qty3
    FROM orders o
    JOIN orderitems oi ON oi.orderid = o.orderid
    JOIN itemsdetail itd ON itd.orderitemsid = oi.orderitemsid
    WHERE o.proddate BETWEEN ? AND ?
        AND itd.grgoodsid = 46110
    GROUP BY o.proddate
)
SELECT
    COALESCE(t1.proddate, t2.proddate, t3.proddate) AS proddate,
    COALESCE(t1.qty1, 0) AS qty_izd_pvh,
    COALESCE(t2.qty2, 0) AS qty_razdv,
    COALESCE(t3.qty3, 0) AS qty_mosnet            
FROM izd_pvh t1
FULL OUTER JOIN razdv t2 ON t1.proddate = t2.proddate
FULL OUTER JOIN mosnet t3 ON COALESCE(t1.proddate, t2.proddate) = t3.proddate
ORDER BY proddate
"""
