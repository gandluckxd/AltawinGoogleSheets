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

# SQL-запросы
SQL_QUERIES = {
    'izd_pvh': """
        SELECT
            o.proddate,
            SUM(oi.qty) AS qty_izd_pvh
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN models m ON m.orderitemsid = oi.orderitemsid
        JOIN r_systems rs ON rs.rsystemid = m.sysprofid
        WHERE o.proddate BETWEEN ? AND ?
            AND rs.systemtype = 0
            AND rs.rsystemid <> 8
        GROUP BY o.proddate
    """,
    'razdv': """
        SELECT
            o.proddate,
            SUM(oi.qty) AS qty_razdv
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN models m ON m.orderitemsid = oi.orderitemsid
        JOIN r_systems rs ON rs.rsystemid = m.sysprofid
        WHERE o.proddate BETWEEN ? AND ?
            AND ((rs.systemtype = 1) OR (rs.rsystemid = 8))
        GROUP BY o.proddate
    """,
    'mosnet': """
        SELECT
            o.proddate,
            SUM(oi.qty * itd.qty) AS qty_mosnet
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN itemsdetail itd ON itd.orderitemsid = oi.orderitemsid
        WHERE o.proddate BETWEEN ? AND ?
            AND itd.grgoodsid = 46110
        GROUP BY o.proddate
    """,
    'glass_packs': """
        select
            o.proddate,
            sum(oi.qty) as qty_glass_packs
        from orders o
        join orderitems oi on oi.orderid = o.orderid
        join models m on m.orderitemsid = oi.orderitemsid
        join modelparts mp on mp.modelid = m.modelid
        join modelfillings mf on mf.modelpartid = mp.modelpartid
        join gpackettypes gp on gp.gptypeid = mf.gptypeid
        join r_systems rs on rs.rsystemid = gp.rsystemid
        where o.proddate between ? and ?
        and rs.rsystemid in (3, 21)
        group by o.proddate
    """,
    'sandwiches': """
        select
            o.proddate,
            sum(oi.qty) as qty_sandwiches
        from orders o
        join orderitems oi on oi.orderid = o.orderid
        join models m on m.orderitemsid = oi.orderitemsid
        join modelparts mp on mp.modelid = m.modelid
        join modelfillings mf on mf.modelpartid = mp.modelpartid
        join gpackettypes gp on gp.gptypeid = mf.gptypeid
        join r_systems rs on rs.rsystemid = gp.rsystemid
        where o.proddate between ? and ?
        and rs.rsystemid in (22)
        group by o.proddate
    """,
    'windowsills': """
        select
            o.proddate,
            sum(i.qty * oi.qty) as qty_windowsills
        from orderitems oi
        join orders o on o.orderid = oi.orderid
        join itemsdetail i on i.orderitemsid = oi.orderitemsid
        join goods g on i.goodsid = g.goodsid
        join groupgoods gg on i.grgoodsid = gg.grgoodsid
        where o.proddate between ? and ?
        and gg.ggtypeid = 42
        group by
            o.proddate
    """,
    'iron': """
        select
            o.proddate,
            sum(oi.qty * its.qty) as qty_iron
        from orderitems oi
        join orders o on o.orderid = oi.orderid
        join itemssets its on its.orderitemsid = oi.orderitemsid
        join groupgoods gg on gg.grgoodsid = its.setid
        where o.proddate between ? and ?
        and gg.isggset = 1
        and ((gg.marking like '%Водоотлив%') or (gg.marking like '%Железо%') or (gg.marking like '%Козырек%') or (gg.marking like '%Нащельник%'))
        group by o.proddate
    """
}
