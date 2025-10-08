import schedule
import time
import logging
from datetime import date, timedelta
from database import get_data_from_db
from google_sheets import update_google_sheet

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def job():
    """
    Основная задача, которая выполняется по расписанию.
    """
    logging.info("Запуск задачи по обновлению данных...")
    
    # Определяем период - за последнюю неделю и на неделю вперед
    today = date.today()
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=7)
    
    # 1. Получаем данные из Firebird
    data = get_data_from_db(start_date, end_date)
    
    # 2. Если данные успешно получены, обновляем Google Sheet
    if data is not None:
        update_google_sheet(data)
    else:
        logging.warning("Пропускаем обновление Google Sheets, так как данные из БД не были получены.")
        
    logging.info("Задача завершена. Следующий запуск завтра.")


if __name__ == "__main__":
    logging.info("Приложение запущено. Первая выгрузка данных начнется немедленно.")
    
    # Запускаем задачу сразу при старте
    job()
    
    # Настраиваем расписание - каждый день в 01:00
    schedule.every().day.at("01:00").do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
