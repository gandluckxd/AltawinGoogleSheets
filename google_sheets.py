import gspread
import logging
from oauth2client.service_account import ServiceAccountCredentials
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
            rows_to_insert = [header]
            
            for i, row_dict in enumerate(processed_new_data):
                row_values = [row_dict.get(h, '') for h in header]
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

        # Создаем карту существующих дат и их номеров строк (1-based index)
        date_to_row_map = {row[date_column_index]: i for i, row in enumerate(sheet_values[1:], start=2)}

        updates_batch = []
        new_rows_to_insert = []

        for row_dict in processed_new_data:
            date_str = row_dict.get('Дата')
            if not date_str:
                continue

            # Собираем значения в том порядке, как они в заголовке на листе
            row_values = [row_dict.get(h, '') for h in current_header]

            if date_str in date_to_row_map:
                row_number = date_to_row_map[date_str]
                
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
            
        # Отображаем только записи в окне: от 2 дней до сегодня и +5 дней
        # Реализуем через скрытие строк вне окна, чтобы избежать конфликтов базового фильтра
        try:
            logging.info("Применение окна отображения по дате (−2 до +5 дней)...")

            # Перечитываем данные после всех изменений, чтобы получить актуальные строки
            latest_values = sheet.get_all_values()
            if not latest_values:
                logging.info("Лист пуст после обновления — нечего фильтровать.")
            else:
                current_header = latest_values[0]
                try:
                    date_column_index = current_header.index('Дата')
                except ValueError:
                    logging.error("Столбец 'Дата' не найден — пропускаю применение окна отображения.")
                    date_column_index = None

                if date_column_index is not None:
                    # Границы окна
                    today = date.today()
                    start_date = today - timedelta(days=2)
                    end_date = today + timedelta(days=5)

                    # Собираем индексы строк (0-based в API; 1-я строка — заголовок) вне окна
                    rows_outside_window_zero_based = []
                    for row_1_based, row in enumerate(latest_values[1:], start=2):
                        raw_date = row[date_column_index] if date_column_index < len(row) else ''
                        try:
                            row_date = datetime.strptime(raw_date, '%d.%m.%Y').date()
                            in_window = (start_date <= row_date <= end_date)
                        except Exception:
                            # Если дата не парсится — скрываем
                            in_window = False

                        if not in_window:
                            # Преобразуем в 0-based индекс строки для API
                            rows_outside_window_zero_based.append(row_1_based - 1)

                    # Сначала показываем все строки (снимаем скрытие)
                    sheet_id = sheet.id if hasattr(sheet, 'id') else sheet._properties.get('sheetId')
                    total_rows = len(latest_values)

                    requests = []
                    if total_rows > 1:
                        requests.append({
                            'updateDimensionProperties': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'dimension': 'ROWS',
                                    'startIndex': 1,   # пропускаем заголовок
                                    'endIndex': total_rows
                                },
                                'properties': {
                                    'hiddenByUser': False
                                },
                                'fields': 'hiddenByUser'
                            }
                        })

                    # Группируем внеоконные строки в непрерывные диапазоны для минимизации запросов
                    def group_contiguous(indices: list[int]) -> list[tuple[int, int]]:
                        if not indices:
                            return []
                        indices.sort()
                        ranges = []
                        start = prev = indices[0]
                        for idx in indices[1:]:
                            if idx == prev + 1:
                                prev = idx
                                continue
                            ranges.append((start, prev + 1))  # end exclusive
                            start = prev = idx
                        ranges.append((start, prev + 1))
                        return ranges

                    hide_ranges_zero_based = group_contiguous(rows_outside_window_zero_based)
                    for start_idx, end_idx in hide_ranges_zero_based:
                        # Не скрываем заголовок; start_idx >= 1 гарантированно
                        if start_idx < 1:
                            start_idx = 1
                        if start_idx >= end_idx:
                            continue
                        requests.append({
                            'updateDimensionProperties': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'dimension': 'ROWS',
                                    'startIndex': start_idx,
                                    'endIndex': end_idx
                                },
                                'properties': {
                                    'hiddenByUser': True
                                },
                                'fields': 'hiddenByUser'
                            }
                        })

                    if requests:
                        spreadsheet.batch_update({'requests': requests})
                        logging.info(
                            "Окно отображения применено: показаны даты от %s до %s, скрыто диапазонов: %d",
                            start_date.strftime('%d.%m.%Y'), end_date.strftime('%d.%m.%Y'),
                            max(0, len(requests) - 1)
                        )
        except Exception as e:
            logging.error(f"Произошла ошибка при применении окна отображения: {e}")

        try:
            logging.info("Обновление времени последнего обновления в ячейке F1...")
            now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            sheet.update('F1', [[f"Последнее обновление: {now}"]])
            logging.info("Время последнего обновления успешно записано в F1.")
        except Exception as e:
            logging.error(f"Не удалось обновить ячейку F1: {e}")

        # Применяем форматирование: шрифт 14 жирный для всей таблицы
        try:
            logging.info("Применение форматирования шрифта (14, жирный)...")
            sheet_id = sheet.id if hasattr(sheet, 'id') else sheet._properties.get('sheetId')
            
            # Перечитываем данные для получения актуального количества строк
            latest_values = sheet.get_all_values()
            total_rows = len(latest_values)
            
            format_requests = []
            
            # Применяем шрифт 14 и жирный ко всей таблице
            if total_rows > 0:
                format_requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': total_rows
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'fontSize': 14,
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold'
                    }
                })
            
            # Находим строку с текущей датой и выделяем её светло-зеленым
            today_str = date.today().strftime('%d.%m.%Y')
            current_header = latest_values[0] if latest_values else []
            
            try:
                date_column_index = current_header.index('Дата')
                for row_idx, row in enumerate(latest_values[1:], start=1):
                    if row[date_column_index] == today_str:
                        # Выделяем строку светло-зеленым цветом
                        format_requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': row_idx,
                                    'endRowIndex': row_idx + 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'backgroundColor': {
                                            'red': 0.85,
                                            'green': 0.92,
                                            'blue': 0.83
                                        },
                                        'textFormat': {
                                            'fontSize': 14,
                                            'bold': True
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold'
                            }
                        })
                        logging.info(f"Найдена и выделена строка с текущей датой: {today_str} (строка {row_idx + 1})")
                        break
            except ValueError:
                logging.warning("Столбец 'Дата' не найден для выделения текущего дня.")
            
            if format_requests:
                spreadsheet.batch_update({'requests': format_requests})
                logging.info("Форматирование успешно применено.")
        except Exception as e:
            logging.error(f"Ошибка при применении форматирования: {e}")

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
