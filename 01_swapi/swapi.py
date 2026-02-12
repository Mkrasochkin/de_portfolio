import requests
import json
from pathlib import Path


class APIRequester:
    """Класс, объекты которого при инициализации должны запоминать атрибут,
    base_url в который будет передаваться базовый URL от какого-либо API.
    """

    def __init__(self, base_url):
        self.base_url = base_url

    def get(self, end_url):
        """Метод выполняет GET-запрос с помощью библиотеки requests, а также
        перехватывает ошибки в случае их возникновения.
        """
        try:
            final_url = f'{self.base_url}{end_url}'
            response = requests.get(final_url)
            response.raise_for_status()
            if response.status_code == 200:
                return response
        except requests.RequestException:
            print('Возникла ошибка при выполнении запроса')


class SWRequester(APIRequester):

    def get_sw_categories(self):
        """Метод выполняет запрос к адресу https://swapi.dev/api/
        и возвращает список доступных категорий,
        например: films, people, planets и т. д.
        """
        response = requests.get(self.base_url + '/')
        op = json.loads(response.text)
        keys_dict = op.keys()
        return keys_dict

    def get_sw_info(self, sw_type):
        """Метод принимает в качестве параметра
        sw_type одну из категорий из предыдущего пункта.
        Метод выполняет запрос и возвращает весь полученный ответ
        в виде строки.
        """
        end_url = f'/{sw_type}/'
        response = self.get(end_url)
        return response.text


def save_sw_data():
    """Функция создает объект класса SWRequester,
    а также директорию под названием data. Для каждой категории SWAPI
    выполняет запрос к ней и сохраняет файл с именем data/<категория>.txt.
    """
    swrequester_object = SWRequester('https://swapi.dev/api')
    Path('data').mkdir(exist_ok=True)
    categories = swrequester_object.get_sw_categories()
    for category in categories:
        result = f'data/{category}.txt'
        categories_info = swrequester_object.get_sw_info(category)
        with open(result, 'w', encoding='utf-8') as file:
            file.write(categories_info)
