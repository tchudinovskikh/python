import requests
from bs4 import BeautifulSoup
import csv
import re

CSV = 'almera1.csv'
HOST = 'https://auto.ru'
URL = 'https://auto.ru/cars/nissan/almera_classic/all/'
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
}

def get_html(url, params=''):
    r = requests.get(url, headers=HEADERS, params=params)
    r.encoding = 'utf-8'
    return r

html = get_html(URL)

print(html)

def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('span', itemtype='http://schema.org/Car')
    cars = []
    for item in items:
        brand = item.find('meta', itemprop='brand').get('content')
        name = item.find('meta', itemprop='name').get('content')
        color = item.find('meta', itemprop='color').get('content')
        fuelType = item.find('meta', itemprop='fuelType').get('content')
        href = item.find('meta', itemprop='url').get('content')
        transmission = item.find('meta', itemprop='vehicleTransmission').get('content')
        q = item.find('meta', itemprop='enginePower').get('content')
        q1 = re.split(r' ', q)
        power = q1[0]
        w = item.find('meta', itemprop='engineDisplacement').get('content')
        w1 = re.split(r' ', w)
        engineDisplacement = w1[0]
        cars.append(
            {
                'brand':brand,
                'name':name,
                'color':color,
                'fuelType':fuelType,
                'href':href,
                'year':item.find('meta', itemprop='productionDate').get('content'),
                'transmission':transmission,
                'power':power,
                'engineDisplacement':engineDisplacement,
                'price': item.find('meta', itemprop='price').get('content')
            }
        )
    return cars
print(get_content(html.text))

def saver(items, path):
    with open(path, 'w', newline='', encoding='cp1251') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Бренд', 'Модель', 'Цвет', 'Тип топлива', 'Ссылка', 'Год', 'Трансмиссия', 'Мощность', 'Объем двигателя', 'Цена'])
        for item in items:
            writer.writerow([item['brand'], item['name'], item['color'], item['fuelType'], item['href'], item['year'], item['transmission'], item['power'], item['engineDisplacement'], item['price']])

def parser():
    PAGENATION = input('укажите кол-во страниц: ')
    PAGENATION = int(PAGENATION.strip())
    html=get_html(URL)
    if html.status_code==200:
        cars = []
        for page in range(1, PAGENATION):
            print(f'in processing, page {page}')
            html = get_html(URL, params={'page': page})
            cars.extend(get_content(html.text))
            saver(cars, CSV)
    else:
        print('Error')

parser()