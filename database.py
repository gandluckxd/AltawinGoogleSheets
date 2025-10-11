import fdb
import logging
from config import DB_CONFIG, SQL_QUERIES
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_from_db(start_date: date, end_date: date) -> list[dict] | None:
    """
    Подключается к базе данных Firebird, выполняет 5 отдельных запросов,
    объединяет результаты и возвращает их.

    Args:
        start_date: Начальная дата для выборки.
        end_date: Конечная дата для выборки.

    Returns:
        Список словарей с данными или None в случае ошибки.
    """
    try:
        logging.info("Подключение к базе данных Firebird...")
        con = fdb.connect(**DB_CONFIG)
        cur = con.cursor()
        
        date1_str = start_date.strftime('%Y-%m-%d')
        date2_str = end_date.strftime('%Y-%m-%d')
        
        all_data = {}

        for key, query in SQL_QUERIES.items():
            logging.info(f"Выполнение SQL-запроса для: {key}...")
            cur.execute(query, (date1_str, date2_str))
            
            columns = [desc[0] for desc in cur.description]
            
            for row in cur.fetchall():
                row_dict = dict(zip(columns, row))
                proddate = row_dict.pop('PRODDATE')
                
                if isinstance(proddate, datetime):
                    proddate = proddate.date()

                if proddate not in all_data:
                    all_data[proddate] = {'PRODDATE': proddate}
                
                all_data[proddate].update(row_dict)

        logging.info(f"Получено и объединено данных по {len(all_data)} датам.")
        
        # Преобразуем словарь в список, который ожидает остальная часть приложения
        return list(all_data.values())

    except fdb.Error as e:
        logging.error(f"Ошибка при работе с базой данных Firebird: {e}")
        return None
    finally:
        if 'con' in locals() and con:
            cur.close()
            con.close()
            logging.info("Соединение с базой данных закрыто.")

if __name__ == '__main__':
    # Пример использования: получить данные за текущий месяц
    today = date.today()
    first_day_of_month = today.replace(day=1)
    
    db_data = get_data_from_db(first_day_of_month, today)
    
    if db_data:
        print("Данные успешно получены:")
        for row in db_data:
            print(row)
