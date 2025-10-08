import gspread
import logging
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import CellFormat, Color, TextFormat, format_cell_range
from config import GOOGLE_SHEETS_CONFIG
from datetime import date, datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_google_sheet(data: list[dict]):
    """
    Авторизуется в Google Sheets и обновляет данные на листе,
    сохраняя существующее форматирование таблицы.
    Ищет строки по дате и обновляет их. Если дата не найдена,
    добавляет новую строку в конец таблицы, наследуя форматирование.

    Args:
        data: Полный список словарей с данными для загрузки.
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
        
        logging.info("Получение существующих данных из таблицы...")
        try:
            sheet_values = sheet.get_all_values()
        except gspread.exceptions.GSpreadException as e:
            logging.warning(f"Не удалось прочитать лист (возможно, он пуст): {e}")
            sheet_values = []

        # 1. Подготавливаем новые данные
        header = ['Дата', 'Изделия', 'Раздвижки', 'Москитные сетки']
        processed_new_data = []
        for row in data:
            processed_row = {}
            for key, value in row.items():
                # Приводим ключи БД к названиям столбцов в таблице
                new_key = {'PRODDATE': 'Дата', 'QTY_IZD_PVH': 'Изделия', 'QTY_RAZDV': 'Раздвижки', 'QTY_MOSNET': 'Москитные сетки'}.get(key, key)
                if isinstance(value, (date, datetime)):
                    processed_row[new_key] = value.strftime('%d.%m.%Y')
                else:
                    processed_row[new_key] = value
            processed_new_data.append(processed_row)

        if not processed_new_data and not sheet_values:
            logging.info("Нет ни существующих, ни новых данных. Лист оставлен пустым.")
            return

        # Если лист пуст, просто вставляем все данные с заголовком
        if not sheet_values:
            logging.info("Лист пуст. Вставляем все данные с заголовком.")
            header.append('ИТОГО')
            rows_to_insert = [header]
            
            izd_col_letter = chr(ord('A') + header.index('Изделия'))
            razdv_col_letter = chr(ord('A') + header.index('Раздвижки'))
            mosnet_col_letter = chr(ord('A') + header.index('Москитные сетки'))

            for i, row_dict in enumerate(processed_new_data):
                row_num = i + 2
                row_values = [row_dict.get(h, '') for h in header if h != 'ИТОГО']
                formula = f"=СУММ({izd_col_letter}{row_num};{razdv_col_letter}{row_num};{mosnet_col_letter}{row_num})"
                row_values.append(formula)
                rows_to_insert.append(row_values)
            
            sheet.update('A1', rows_to_insert, value_input_option='USER_ENTERED')
            logging.info("Данные успешно загружены.")
            return

        # Если лист не пуст, выполняем обновление/добавление
        current_header = sheet_values[0]
        try:
            date_column_index = current_header.index('Дата')
        except ValueError:
            logging.error("На листе отсутствует столбец 'Дата'. Невозможно выполнить обновление.")
            return

        # Готовим шаблон для формулы ИТОГО, если столбец существует
        formula_template = None
        total_col_idx = -1
        if 'ИТОГО' in current_header:
            try:
                total_col_idx = current_header.index('ИТОГО')
                izd_col_letter = chr(ord('A') + current_header.index('Изделия'))
                razdv_col_letter = chr(ord('A') + current_header.index('Раздвижки'))
                mosnet_col_letter = chr(ord('A') + current_header.index('Москитные сетки'))
                formula_template = f"=СУММ({izd_col_letter}{{row_num}};{razdv_col_letter}{{row_num}};{mosnet_col_letter}{{row_num}})"
            except ValueError:
                logging.warning("Не удалось найти все столбцы ('Изделия', 'Раздвижки', 'Москитные сетки') для формулы 'ИТОГО'.")

        # Создаем карту существующих дат и их номеров строк (1-based index)
        date_to_row_map = {row[date_column_index]: i for i, row in enumerate(sheet_values[1:], start=2)}

        updates_batch = []
        new_rows_to_insert = []

        for row_dict in processed_new_data:
            date_str = row_dict.get('Дата')
            if not date_str:
                continue

            # Собираем значения в том порядке, как они в заголовке на листе
            row_values = []
            for h in current_header:
                if h == 'ИТОГО' and formula_template:
                    row_values.append('')  # Заполнитель для формулы
                else:
                    row_values.append(row_dict.get(h, ''))

            if date_str in date_to_row_map:
                row_number = date_to_row_map[date_str]
                if formula_template:
                    formula = formula_template.format(row_num=row_number)
                    row_values[total_col_idx] = formula

                updates_batch.append({
                    'range': f'A{row_number}:{chr(ord("A")+len(current_header)-1)}{row_number}',
                    'values': [row_values]
                })
            else:
                # Если такой даты нет, это новая строка
                # Проверяем, чтобы эта дата не была уже в списке на добавление
                if date_str not in [r[date_column_index] for r in new_rows_to_insert]:
                     new_rows_to_insert.append(row_values)
        
        if updates_batch:
            logging.info(f"Обновление {len(updates_batch)} существующих строк...")
            sheet.batch_update(updates_batch, value_input_option='USER_ENTERED')

        if new_rows_to_insert:
            # Добавляем формулы в новые строки перед вставкой
            if formula_template:
                start_row = len(sheet_values) + 1
                for i, row in enumerate(new_rows_to_insert):
                    row_num = start_row + i
                    formula = formula_template.format(row_num=row_num)
                    row[total_col_idx] = formula

            # Сортируем новые строки по дате перед вставкой
            try:
                new_rows_to_insert.sort(key=lambda r: datetime.strptime(r[date_column_index], '%d.%m.%Y'))
            except (ValueError, IndexError):
                logging.warning("Не удалось отсортировать новые строки перед вставкой.")

            logging.info(f"Добавление {len(new_rows_to_insert)} новых строк в конец таблицы...")
            # Вставляем строки после последней существующей строки, наследуя форматирование
            sheet.insert_rows(
                new_rows_to_insert,
                row=len(sheet_values) + 1,
                value_input_option='USER_ENTERED',
                inherit_from_before=True
            )
            
        # Обновляем фильтр после всех операций, управляя видимостью строк
        try:
            logging.info("Обновление видимости строк (фильтрация)...")
            all_values = sheet.get_all_values()
            num_rows = len(all_values)
            if num_rows <= 1:
                logging.info("Недостаточно данных для фильтрации.")
                return

            sheet_header = all_values[0]
            
            # Находим индексы нужных столбцов
            try:
                date_col_idx = sheet_header.index('Дата')
                izd_col_idx = sheet_header.index('Изделия')
                razdv_col_idx = sheet_header.index('Раздвижки')
                mosnet_col_idx = sheet_header.index('Москитные сетки')
            except ValueError as e:
                logging.error(f"Не найден необходимый столбец для фильтрации: {e}")
                spreadsheet.batch_update({"requests": [{"clearBasicFilter": {"sheetId": sheet.id}}]}) # На всякий случай сбрасываем старый фильтр
                return

            # Определяем диапазон дат для фильтра
            start_date_filter = date.today() - timedelta(days=2)
            end_date_filter = date.today() + timedelta(days=5)

            rows_to_hide = []
            # Пропускаем заголовок (индекс 0), итерируемся по строкам данных
            for i, row in enumerate(all_values[1:], start=1): # start=1, т.к. API использует 0-based, а мы пропускаем заголовок
                
                # Проверяем, что в строке есть данные
                if len(row) <= max(date_col_idx, izd_col_idx, razdv_col_idx, mosnet_col_idx):
                    rows_to_hide.append(i) # Скрываем пустые/некорректные строки
                    continue

                # --- Проверка по дате ---
                is_in_date_range = False
                try:
                    row_date = datetime.strptime(row[date_col_idx], '%d.%m.%Y').date()
                    if start_date_filter <= row_date <= end_date_filter:
                        is_in_date_range = True
                except (ValueError, IndexError):
                    pass # Если дата в неверном формате или отсутствует, считаем, что она не в диапазоне

                # --- Проверка по нулевым значениям ---
                has_values = False
                try:
                    izd_val = int(row[izd_col_idx] or 0)
                    razdv_val = int(row[razdv_col_idx] or 0)
                    mosnet_val = int(row[mosnet_col_idx] or 0)
                    if izd_val > 0 or razdv_val > 0 or mosnet_val > 0:
                        has_values = True
                except (ValueError, IndexError):
                    pass
                
                # Строку нужно скрыть, если она НЕ удовлетворяет ОБОИМ условиям
                if not (is_in_date_range and has_values):
                    rows_to_hide.append(i)

            requests = [
                # 1. Сначала удаляем старый базовый фильтр, если он есть
                {"clearBasicFilter": {"sheetId": sheet.id}},
                # 2. Показываем все строки данных (от второй строки до конца)
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "ROWS",
                            "startIndex": 1, 
                            "endIndex": num_rows
                        },
                        "properties": {"hiddenByUser": False},
                        "fields": "hiddenByUser"
                    }
                }
            ]

            # 3. Скрываем строки, которые не прошли фильтрацию
            for row_idx in rows_to_hide:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet.id,
                            "dimension": "ROWS",
                            "startIndex": row_idx,
                            "endIndex": row_idx + 1
                        },
                        "properties": {"hiddenByUser": True},
                        "fields": "hiddenByUser"
                    }
                })
            
            if len(requests) > 1: # Отправляем запрос, только если есть что делать
                spreadsheet.batch_update({"requests": requests})
                logging.info(f"Фильтрация применена. Обработано строк: {num_rows - 1}. Скрыто: {len(rows_to_hide)}.")

        except Exception as e:
            logging.error(f"Произошла комплексная ошибка при обновлении видимости строк: {e}")

        try:
            logging.info("Обновление времени последнего обновления в ячейке F1...")
            now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            sheet.update('F1', [[f"Последнее обновление: {now}"]])
            logging.info("Время последнего обновления успешно записано в F1.")
        except Exception as e:
            logging.error(f"Не удалось обновить ячейку F1: {e}")

        logging.info("Обновление данных в Google Sheets завершено.")

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
