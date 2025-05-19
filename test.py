import json

arr = []

with open('data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)  # Загружаем весь JSON файл как список словарей
    for item in data:  # Итерируем по каждому элементу списка
        tariff_type = item['tariff_parsed']['type']
        if tariff_type not in arr:
            arr.append(tariff_type)

print(arr)