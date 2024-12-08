import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.
    Эта функция отправляет запрос к API Ozon для получения списка товаров
    в магазине с указанным клиентским ID. Она позволяет фильтровать товары
    по видимости и поддерживает постраничный вывод.

    Аргументы:
        last_id: Идентификатор последнего товара. Используется для
            постраничного вывода. Если это первый вызов функции, можно
            передать 0 или None.
        client_id: Уникальный идентификатор клиента для аутентификации
            при обращении к API Ozon.
        seller_token: Токен продавца для доступа к защищённым ресурсам
            API Ozon.

    Возвращает:
        Список товаров, возвращенный API Ozon. Каждый товар представлен
        как словарь с соответствующей информацией.
        """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.
    Эта функция использует API Ozon для получения списка товаров
    и извлекает артикулы (offer_id) из полученного списка. Функция работает 
    в цикле до тех пор, пока не будут получены все товары, а затем возвращает 
    список артикулов.

    Аргументы:
        client_id: Уникальный идентификатор клиента для аутентификации 
                    при обращении к API Ozon.
        seller_token: Токен продавца для доступа к API Ozon.

    Возвращает:
        Список артикулов (offer_id) товаров, доступных в магазине Ozon.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.
    Эта функция отправляет запрос к API Ozon для обновления цен на товары,
    используя переданный список цен, уникальный идентификатор клиента и токен
    продавца для аутентификации.

    Аргументы:
        prices: Список словарей с информацией о ценах товаров.
                    Каждый словарь должен содержать идентификатор предложения (offer_id)
                    и новую цену (price), например:
                    [{"offer_id": "123", "price": 5990}, {"offer_id": "456", "price": 7500}].
        client_id: Уникальный идентификатор клиента для аутентификации
                        при обращении к API Ozon.
        seller_token: Токен продавца для доступа к защищённым ресурсам
                        API Ozon.

    Возвращает ответ от API Ozon, содержащий информацию о результате работы функции.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.
    Эта функция отправляет запрос к API Ozon для обновления остатков товаров,
    используя переданный список остатков, уникальный идентификатор клиента и 
    токен продавца для аутентификации.

    Аргументы:
        stocks: Список словарей с информацией об остатках товаров. 
                Каждый словарь должен содержать идентификатор предложения 
                (offer_id) и новое количество (quantity), например:
                [{"offer_id": "123", "quantity": 10}, {"offer_id": "456", "quantity": 5}].
        client_id: Уникальный идентификатор клиента для аутентификации 
                    при обращении к API Ozon.
        seller_token: Токен продавца для доступа к защищённым ресурсам API Ozon.

    Возвращает ответ от API Ozon, содержащий информацию о результате операции.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.
    Эта функция осуществляет HTTP-запрос к ресурсу по указанному URL для загрузки 
    ZIP-архива. После успешной загрузки 
    она извлекает содержимое архива, читает данные из файла Excel и формирует 
    список остатков товаров, представленный в виде списка словарей. В конце 
    функция удаляет временный файл Excel.

    Возвращает список остатков товаров, где каждый элемент представляет 
    собой словарь с данными о товаре.
"""
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков для товаров на основании загруженных данных и идентификаторов предложений.

    Эта функция принимает список остатков часов (watch_remnants), извлеченных из файла,
    и список идентификаторов предложений (offer_ids), чтобы сформировать конечный
    список остатков, соответствующих каждому товару. Функция фильтрует остатки,
    убирая те, которые не загружены в систему, и добавляет записи с нулевыми
    остатками для тех предложений, которые отсутствуют в загруженных данных.

    Аргументы:
        watch_remnants: Список словарей, где каждый словарь содержит
            информацию о товаре, включая его код и количество остатка.
        offer_ids: Список идентификаторов предложений (offer_id), для
            которых необходимо создать остатки.

    Возвращает cписок словарей с информацией об остатках на продукцию, где каждый
            словарь содержит ключи 'offer_id' и 'stock'.
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для часов на основе остатка и идентификаторов предложений.

    Аргументы:
        watch_remnants: Список словарей, представляющих остатки часов, 
                            где каждый словарь содержит информацию о модели, 
                            включая "Код" и "Цена".
        offer_ids: Множество идентификаторов предложений, по которым 
                     будет производиться фильтрация остатков.

    Возвращает:
        prices: Список словарей, где каждый словарь содержит информацию о цене 
              часов, включая идентификатор предложения и цену, обработанную 
              функцией `price_conversion()`. Каждый словарь имеет следующие ключи:
              - "auto_action_enabled": статус авто-действия ("UNKNOWN"),
              - "currency_code": код валюты ("RUB"),
              - "offer_id": идентификатор предложения (код часов из watch_remnants),
              - "old_price": старая цена ("0"),
              - "price": новая цена, преобразованная в формат целого числа.

    Пример:
        >>> watch_remnants = [
        ...     {"Код": 123, "Цена": "5'990.00"},
        ...     {"Код": 456, "Цена": "7'500.00"},
        ... ]
        >>> offer_ids = {"123", "789"}
        >>> create_prices(watch_remnants, offer_ids)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену. Пример: 5'990.00 руб. -> 5990.
    Аргументы:
        price : Строка, содержащая цену, которая может включать 
                разделители (например, запятые, пробелы и знаки рубля) 
                и десятичную часть.
                Пример: "5'990.00 руб."

    Возвращает:
        str: Целое число в строковом формате без разделителей и десятичной части.
             Пример: "5990".

    Примечание:
        Функция удаляет все символы, кроме цифр, из целой части цены, 
        и возвращает результат как строку.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов
    Аргументы:
        lst (list): Список, который нужно разделить.
        n (int): Количество элементов в каждой части.

    Генерирует:
        list: Подсписки из n элементов. Последний подсписок может содержать
        меньше элементов, если длина списка не кратна n.
"""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
