import schedule
import time
import logging
from datetime import date, timedelta, datetime
from database import get_data_from_db
from google_sheets import update_google_sheet

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def job():
    """
    Основная задача, которая выполняется по расписанию.
    """
    logging.info("Запуск задачи по обновлению данных...")
    
    # Определяем период - за последние 14 дней и на 10 дней вперед
    today = date.today()
    start_date = today - timedelta(days=14)
    end_date = today + timedelta(days=14)
    
    # 1. Получаем данные из Firebird
    db_data = get_data_from_db(start_date, end_date)
    
    # 2. Если данные успешно получены, обрабатываем их и обновляем Google Sheet
    if db_data is not None:
        # Создаем полный список дат за период
        all_dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
        
        # Преобразуем данные из БД в словарь для быстрого доступа по дате,
        # нормализуя ключ к типу date.
        db_data_map = {}
        for row in db_data:
            db_date = row['PRODDATE']
            # Firebird может возвращать datetime, а мы сравниваем с date
            if isinstance(db_date, datetime):
                db_date = db_date.date()
            db_data_map[db_date] = row
        
        full_data = []
        for dt in all_dates:
            if dt in db_data_map:
                row = db_data_map[dt]
                full_data.append({
                    'PRODDATE': row.get('PRODDATE', dt),
                    'QTY_IZD_PVH': row.get('QTY_IZD_PVH', 0),
                    'QTY_RAZDV': row.get('QTY_RAZDV', 0),
                    'QTY_MOSNET': row.get('QTY_MOSNET', 0),
                    'QTY_GLASS_PACKS': row.get('QTY_GLASS_PACKS', 0),
                    'QTY_SANDWICHES': row.get('QTY_SANDWICHES', 0),
                    'QTY_WINDOWSILLS': row.get('QTY_WINDOWSILLS', 0),
                    'QTY_IRON': row.get('QTY_IRON', 0)
                })
            else:
                # Если данных за эту дату нет, добавляем строку с нулями
                full_data.append({
                    'PRODDATE': dt,
                    'QTY_IZD_PVH': 0,
                    'QTY_RAZDV': 0,
                    'QTY_MOSNET': 0,
                    'QTY_GLASS_PACKS': 0,
                    'QTY_SANDWICHES': 0,
                    'QTY_WINDOWSILLS': 0,
                    'QTY_IRON': 0
                })
        
        update_google_sheet(full_data)
    else:
        logging.warning("Пропускаем обновление Google Sheets, так как данные из БД не были получены.")
        
    logging.info("Задача завершена. Следующий запуск через 5 минут.")


if __name__ == "__main__":
    logging.info("Приложение запущено. Первая выгрузка данных начнется немедленно.")
    
    # Запускаем задачу сразу при старте
    job()
    
    # Настраиваем расписание - каждые 5 минут
    schedule.every(5).minutes.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
