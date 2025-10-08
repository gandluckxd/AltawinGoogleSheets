import fdb
import logging
from config import DB_CONFIG, SQL_QUERY
from datetime import date

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_from_db(start_date: date, end_date: date) -> list[dict] | None:
    """
    Подключается к базе данных Firebird, выполняет запрос и возвращает данные.

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
        logging.info("Выполнение SQL-запроса...")
        
        # Преобразуем даты в строки в формате, который ожидает Firebird
        date1_str = start_date.strftime('%Y-%m-%d')
        date2_str = end_date.strftime('%Y-%m-%d')
        
        # Firebird fdb драйвер использует '?' как плейсхолдер
        cur.execute(SQL_QUERY, (date1_str, date2_str, date1_str, date2_str, date1_str, date2_str))
        
        # Получаем названия столбцов
        columns = [desc[0] for desc in cur.description]
        
        # Формируем результат в виде списка словарей
        data = []
        data = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        logging.info(f"Получено {len(data)} строк из базы данных.")
        
        return data

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
