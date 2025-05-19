import pandas as pd
import re
import json

def parse_tariff(tariff_text):
    """Парсит тарифную ставку в структурированный формат"""
    if not tariff_text or pd.isna(tariff_text) or tariff_text == '-':
        return {"type": "unknown", "raw": ""}
    
    tariff_text = str(tariff_text).strip()
    result = {"raw": tariff_text}
    
    # Парсим процентную ставку (адвалорную)
    percent_match = re.search(r'(\d+(?:[.,]\d+)?)\s*%', tariff_text)
    if percent_match:
        result["advalorem_percent"] = float(percent_match.group(1).replace(',', '.'))
        result["type"] = "advalorem"
    
    # Парсим специфическую ставку в евро
    euro_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:EUR|евро|EUR\s+\d+)', tariff_text, re.IGNORECASE)
    if euro_match:
        result["specific_euro"] = float(euro_match.group(1).replace(',', '.'))
        if "type" not in result:
            result["type"] = "specific"
        elif result["type"] == "advalorem":
            result["type"] = "combined"
    
    # Проверяем на минимальную ставку
    min_match = re.search(r'(?:но не менее|min)\s+(\d+(?:[.,]\d+)?)\s*(?:EUR|евро)', tariff_text, re.IGNORECASE)
    if min_match:
        result["minimum_euro"] = float(min_match.group(1).replace(',', '.'))
        if result["type"] == "combined":
            result["type"] = "combined_max"
    
    # Если тип не определен, но есть числовое значение
    if "type" not in result and re.search(r'\d+(?:[.,]\d+)?', tariff_text):
        numeric_value = re.search(r'(\d+(?:[.,]\d+)?)', tariff_text).group(1)
        result["value"] = float(numeric_value.replace(',', '.'))
        result["type"] = "unknown_numeric"
    
    # Если тип все еще не определен
    if "type" not in result:
        result["type"] = "unknown"
    
    return result

def process_csv_file(file_path):
    """Обрабатывает CSV-файл с разными подходами"""
    # Попробуем несколько методов чтения файла
    
    # 1. Пробуем прочитать как стандартный CSV с разными разделителями
    for sep in [',', ';', '\t', '|']:
        try:
            df = pd.read_csv(file_path, sep=sep, dtype=str)
            # Если успешно прочитали и есть хотя бы две колонки
            if len(df.columns) >= 2:
                print(f"Успешно прочитали файл с разделителем '{sep}'")
                return process_dataframe(df)
        except Exception:
            pass
    
    # 2. Пробуем прочитать с фиксированной шириной
    try:
        df = pd.read_fwf(file_path, dtype=str)
        if len(df.columns) >= 2:
            print(f"Успешно прочитали файл как файл с фиксированной шириной")
            return process_dataframe(df)
    except Exception:
        pass
    
    # 3. Читаем как обычный текстовый файл и обрабатываем построчно
    print("Попытка обработки как текстового файла построчно...")
    return process_text_file(file_path)

def process_dataframe(df):
    """Обрабатывает DataFrame для извлечения кодов ТН ВЭД и тарифов"""
    tnved_data = []
    
    # Определяем колонки для кода и тарифа
    code_column = None
    tariff_column = None
    name_column = None
    
    # Ищем колонки по названию или позиции
    for col in df.columns:
        col_lower = str(col).lower()
        if 'код' in col_lower or 'code' in col_lower or col_lower == '0':
            code_column = col
        elif 'тариф' in col_lower or 'tariff' in col_lower or 'ставк' in col_lower:
            tariff_column = col
        elif 'наим' in col_lower or 'name' in col_lower or 'опис' in col_lower:
            name_column = col
    
    # Если не нашли колонки по названию, используем позиции
    if not code_column and len(df.columns) > 0:
        code_column = df.columns[0]
    
    if not tariff_column and len(df.columns) > 1:
        # Ищем колонку с тарифами
        for col in df.columns:
            if col == code_column or col == name_column:
                continue
                
            # Проверяем, содержит ли колонка проценты или евро
            if df[col].astype(str).str.contains('%|EUR|евро', case=False, regex=True).any():
                tariff_column = col
                break
        
        # Если не нашли, берем вторую колонку
        if not tariff_column and len(df.columns) > 1:
            for col in df.columns:
                if col != code_column and col != name_column:
                    tariff_column = col
                    break
    
    print(f"Используются колонки: код='{code_column}', тариф='{tariff_column}', название='{name_column}'")
    
    # Обрабатываем каждую строку
    for _, row in df.iterrows():
        code = str(row[code_column]).strip() if code_column else ""
        tariff = str(row[tariff_column]).strip() if tariff_column else ""
        name = str(row[name_column]).strip() if name_column and name_column in row else ""
        
        # Проверяем формат кода ТН ВЭД
        if code and re.match(r'^\d{4,10}', code):
            parsed_tariff = parse_tariff(tariff)
            tnved_data.append({
                "code": code,
                "name": name,
                "tariff_raw": tariff,
                "tariff_parsed": parsed_tariff
            })
    
    return tnved_data

def process_text_file(file_path):
    """Обрабатывает текстовый файл построчно для извлечения кодов ТН ВЭД и тарифов"""
    tnved_data = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                
                # Пропускаем пустые строки и разделители
                if not line or line.startswith('---'):
                    continue
                
                # Ищем код ТН ВЭД (10 цифр или менее, но не менее 4)
                code_match = re.search(r'\b(\d{4,10})\b', line)
                if code_match:
                    code = code_match.group(1)
                    
                    # Ищем тариф (процент, евро или просто число)
                    tariff = ""
                    tariff_match = re.search(r'(\d+(?:[.,]\d+)?\s*%)', line)
                    if tariff_match:
                        tariff = tariff_match.group(1)
                    else:
                        tariff_match = re.search(r'(\d+(?:[.,]\d+)?\s*(?:EUR|евро)(?:\s+\d+)?)', line, re.IGNORECASE)
                        if tariff_match:
                            tariff = tariff_match.group(1)
                    
                    # Если тариф не найден, ищем число в конце строки
                    if not tariff:
                        parts = line.split()
                        for part in reversed(parts):
                            if re.match(r'^\d+(?:[.,]\d+)?$', part):
                                tariff = part
                                break
                    
                    # Название товара - сложно определить в текстовом файле
                    name = ""
                    
                    parsed_tariff = parse_tariff(tariff)
                    tnved_data.append({
                        "code": code,
                        "name": name,
                        "tariff_raw": tariff,
                        "tariff_parsed": parsed_tariff
                    })
    except Exception as e:
        print(f"Ошибка при обработке текстового файла: {e}")
    
    return tnved_data

def save_json(data, output_file):
    """Сохраняет данные в JSON файл"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    input_file = "TWS_TNVED_2025-05-18.csv"
    output_file = "tnved_data.json"
    
    print(f"Начинаем обработку файла {input_file}...")
    tnved_data = process_csv_file(input_file)
    
    print(f"Найдено {len(tnved_data)} записей")
    save_json(tnved_data, output_file)
    print(f"Данные сохранены в {output_file}")
    
    # Выводим примеры первых нескольких записей
    if tnved_data:
        print("\nПримеры записей:")
        for i, item in enumerate(tnved_data[:5]):
            print(f"{i+1}. Код: {item['code']}, Тариф: {item['tariff_raw']}")
            print(f"   Распознанный тариф: {item['tariff_parsed']}")
    else:
        print("Не удалось извлечь данные из файла.")

if __name__ == "__main__":
    main()
