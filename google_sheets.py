import gspread
import logging
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import CellFormat, Color, TextFormat, format_cell_range
from config import GOOGLE_SHEETS_CONFIG
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_google_sheet(data: list[dict]):
    """
    Авторизуется в Google Sheets и обновляет данные на листе.
    
    Args:
        data: Список словарей с данными для загрузки.
    """
    try:
        logging.info("Авторизация в Google Sheets...")
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GOOGLE_SHEETS_CONFIG['credentials_file'], scope
        )
        client = gspread.authorize(creds)

        logging.info(f"Открытие таблицы '{GOOGLE_SHEETS_CONFIG['spreadsheet_name']}'...")
        spreadsheet = client.open(GOOGLE_SHEETS_CONFIG['spreadsheet_name'])
        sheet = spreadsheet.worksheet(GOOGLE_SHEETS_CONFIG['worksheet_name'])
        
        logging.info("Очистка листа...")
        sheet.clear()

        if not data:
            logging.info("Нет данных для загрузки. Лист оставлен пустым.")
            return

        logging.info(f"Загрузка {len(data)} строк в таблицу...")

        # Переименовываем столбцы и форматируем дату
        processed_data = []
        column_mapping = {
            'PRODDATE': 'Дата',
            'QTY_IZD_PVH': 'Изделия',
            'QTY_RAZDV': 'Раздвижки',
            'QTY_MOSNET': 'Москитные сетки'
        }
        
        for row in data:
            processed_row = {}
            for key, value in row.items():
                new_key = column_mapping.get(key, key)
                if isinstance(value, (date, datetime)):
                    processed_row[new_key] = value.strftime('%d.%m')
                else:
                    processed_row[new_key] = value
            processed_data.append(processed_row)

        header = list(processed_data[0].keys())
        rows_to_insert = [header] + [list(row.values()) for row in processed_data]
        
        sheet.insert_rows(rows_to_insert, 1)
        
        # Форматирование заголовка
        fmt = CellFormat(
            backgroundColor=Color(0.9, 0.9, 0.9),
            textFormat=TextFormat(bold=True)
        )
        format_cell_range(sheet, f'A1:{chr(ord("A")+len(header)-1)}1', fmt)

        # Создание диаграммы
        chart = sheet.new_chart()
        chart.title = 'Производство по дням'
        chart.chart_type = 'COLUMN'
        chart.stacked = True
        chart.set_range(f'A1:{chr(ord("A")+len(header)-1)}{len(rows_to_insert)}')
        chart.add_series({
            'source': {'start': (0, 1), 'end': (len(rows_to_insert), 2)},
            'name': 'Изделия'
        })
        chart.add_series({
            'source': {'start': (0, 2), 'end': (len(rows_to_insert), 3)},
            'name': 'Раздвижки'
        })
        chart.add_series({
            'source': {'start': (0, 3), 'end': (len(rows_to_insert), 4)},
            'name': 'Москитные сетки'
        })
        chart.anchor_cell = (2, len(header) + 2)
        sheet.add_chart(chart)
        
        logging.info("Данные успешно загружены и отформатированы в Google Sheets.")

    except FileNotFoundError:
        logging.error(f"Файл {GOOGLE_SHEETS_CONFIG['credentials_file']} не найден. "
                      f"Пожалуйста, убедитесь, что он находится в корневом каталоге проекта.")
    except Exception as e:
        logging.error(f"Произошла ошибка при работе с Google Sheets: {e}")

if __name__ == '__main__':
    # Пример использования:
    # Для запуска этого примера, убедитесь, что у вас есть credentials.json
    # и вы предоставили доступ сервисному аккаунту к вашей таблице.
    
    # Пример данных
    sample_data = [
        {'PRODDATE': date(2023, 10, 1), 'QTY_IZD_PVH': 10, 'QTY_RAZDV': 5, 'QTY_MOSNET': 20},
        {'PRODDATE': date(2023, 10, 2), 'QTY_IZD_PVH': 12, 'QTY_RAZDV': 8, 'QTY_MOSNET': 22},
    ]
    
    update_google_sheet(sample_data)
    # print("Для тестирования этого модуля раскомментируйте вызов update_google_sheet "
    #       "и убедитесь в наличии credentials.json")
