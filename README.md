# ozon-price-excel (manual sync)

Скрипт выгружает товары из Ozon Seller API + данные из МойСклад и записывает в Google Sheet (лист `API Ozon`).

## Колонки (в таком порядке)
1. Cabinet
2. Категория товара нижнего уровня
3. Тип товара
4. Название товара (МойСклад)
5. offer_id
6. Закупочная цена
7. Цена до скидок
8. Минимальная цена
9. Ваша цена (marketing_seller_price)
10. Цена для покупателя

Сортировка в программе: сначала по категории, затем внутри категории по типу, затем внутри типа по названию товара.

## Установка

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Настройка
1) Скопируйте `config.example.env` в `.env`.
2) Заполните `.env`:
- `SPREADSHEET_ID` (берётся из URL таблицы)
- `WORKSHEET_NAME` (по умолчанию `API Ozon`)
- `GOOGLE_SERVICE_ACCOUNT_JSON` (путь к json ключу сервис-аккаунта)
- `MS_TOKEN` (токен МойСклад)
- `OZON_CLIENT_ID_1` / `OZON_API_KEY_1` (Cab1)
- (опционально) `OZON_CLIENT_ID_2` / `OZON_API_KEY_2` (Cab2)

Важно: сервис-аккаунту нужно дать доступ на таблицу (поделиться таблицей на email из json).

## Запуск
```bash
python sync.py
```

При успешном выполнении скрипт очистит лист и запишет обновлённые строки.
