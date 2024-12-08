import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров из Яндекс.Маркет.

    Эта функция выполняет HTTP-запрос к API Яндекс.Маркета для получения списка
    товаров, связанных с указанной рекламной кампанией. Она принимает 
    параметры для постраничной навигации, идентификатор кампании и токен доступа
    для аутентификации. Функция возвращает результаты запроса в виде списка.

    Аргументы:
        page (str): Токен страницы для постраничной навигации. Позволяет 
            получать результаты с разных страниц.
        campaign_id (str): Уникальный идентификатор рекламной кампании,
            для которой нужно получить список товаров.
        access_token (str): Токен доступа для аутентификации при запросе к API.

    Возвращает:
        list: список товаров в соответствии с параметрами запроса. 
              Возвращает None, если в ответе отсутствуют результаты.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров на Яндекс.Маркет.

    Эта функция отправляет обновленные остатки товаров на сервер Яндекс.Маркет 
    с использованием API. Она принимает список остатков товаров, идентификатор 
    кампании и токен доступа для аутентификации. Функция выполняет запрос 
    на обновление остатков и возвращает ответ от API.

    Аргументы:
        stocks (list): Список обновленных остатков товара, каждый элемент должен 
                    иметь структуру, соответствующую требованиям API, например, 
                    список SKU и их остатков.
        campaign_id (str): Идентификатор рекламной кампании, к которой 
                         относятся обновления остатков товаров.
        access_token (str): Токен доступа для аутентификации при обращении к API.

    Возвращает:
        dict: ответ от API, содержащий результаты операции обновления.
"""
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров на Яндекс.Маркете.

    Эта функция отправляет обновленные цены товаров на сервер Яндекс.Маркет, используя API. 
    Она принимает список цен товаров, идентификатор рекламной кампании и токен доступа 
    для аутентификации. Функция выполняет запрос на обновление цен и возвращает 
    ответ от API.

    Аргументы:
        prices (list): Список обновленных цен товаров. Каждый элемент должен иметь структуру, 
                       соответствующую требованиям API, включая SKU и новую цену.
        campaign_id (str): Уникальный идентификатор рекламной кампании, к которой 
                           относятся обновления цен товаров.
        access_token (str): Токен доступа для аутентификации при обращении к API.

    Возвращает:
        dict: ответ от API, содержащий результаты операции обновления.
"""
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета.
    Эта функция выполняет запрос к API Яндекс.Маркета для получения всех
    товаров, связанных с указанной рекламной кампанией. Функция собирает
    данные постранично, используя токены страниц, и извлекает артикулы
    (sku) каждого товара. Возвращается список артикулов товаров.

    Аргументы:
        campaign_id (str): Уникальный идентификатор рекламной кампании, для
                       которой нужно получить артикулы товаров.
        market_token (str): Токен доступа для аутентификации при запросе
                        к API Яндекс.Маркета.

    Возвращает:
        list: список артикулов товаров, относящихся к указанной
              кампании. Если товаров нет, возвращает пустой список.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает список остатков товаров для обновления в Yandex Market.

    Эта функция формирует структуру данных для остатков товаров,
    основанную на имеющихся остатках (watch_remnants) и загруженных
    артикулов (offer_ids). Остатки формируются в соответствии с
    условиями, указанными для каждого товара, и добавляются в специальный
    формат, ожидаемый API Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список остатков товаров, полученных из
            другого источника, где каждый элемент является словарем, 
            содержащим данные о товаре (например, "Код" и "Количество").
        offer_ids (str): Список артикулов (SKU), которые должны быть
            учтены при формировании остатков.
        warehouse_id (str): Идентификатор склада, к которому относятся
            формируемые остатки.

    Возвращает:
        list: список остатков, форматированный для передачи в API Яндекс.Маркет. Каждый элемент списка — это словарь,
            содержащий информацию
            об артикуле, идентификаторе склада и остатков.
"""
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список объектов цен для товаров на основе остатков.

    Эта функция формирует структуру данных для цен товаров, 
    используя информацию об остатках (watch_remnants) и 
    доступных артикулах (offer_ids). Функция проверяет, 
    присутствует ли код товара из остатков в списке загруженных 
    артикулов, и если да, добавляет его цену в формат, 
    ожидаемый API Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список с остатками товаров, где каждый 
        элемент представляет собой словарь с данными о товаре, 
        включая "Код" и "Цена".
    
        offer_ids (list): Список артикулов (SKU), для которых необходимо 
        создать объекты цен.

    Возвращает:
        list: список объектов цен, каждый из которых содержит идентификатор товара, 
    его цену и валюту. Если товаров нет в списке артикулов, 
    возвращается пустой список.
"""
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
