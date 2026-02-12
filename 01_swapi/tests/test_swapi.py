from contextlib import contextmanager

import pytest
import requests
import requests_mock
import requests_mock.exceptions


try:
    import swapi
except ModuleNotFoundError:
    assert False, "Не найдена домашняя работа"

init_base_url = [
    {
        "base_url": "https://swapi.dev/api",
        "text": "test text 1",
        "json": {"key1": "v1", "key2": "v2"},
    },
    {
        "base_url": "https://ya.ru/t",
        "text": "test text 2",
        "json": {"key8": "v8", "key9": "v9"},
    },
]


class TestAPIRequester:
    @pytest.mark.parametrize("kwargs", init_base_url)
    def test_init(self, kwargs, msg_err):
        assert hasattr(swapi, "APIRequester"), msg_err("add_class", "APIRequester")  # noqa

        result = swapi.APIRequester(kwargs["base_url"])
        assert hasattr(result, "base_url"), msg_err(
            "add_attr", "base_url", "APIRequester"
        )
        assert result.base_url == kwargs["base_url"], msg_err(
            "wrong_attr", "base_url", "APIRequester"
        )

    @pytest.mark.parametrize("kwargs", init_base_url)
    def test_get(self, kwargs, msg_err, capfd):
        result = swapi.APIRequester(kwargs["base_url"])
        assert hasattr(result, "get"), msg_err("add_method", "get", "APIRequester")  # noqa

        with requests_mock.Mocker() as m:
            url = f"{kwargs['base_url']}/url"
            m.get(url, json={"name": "get-mock"})

            try:
                resp = result.get("/url")
            except requests_mock.exceptions.NoMockAddress:
                assert (
                    m.last_request.url == url
                ), "Убедитесь, что правильно формируете адрес из переданных параметров в методе `get` класса `APIRequester`"  # noqa

            assert resp.json() == {
                "name": "get-mock"
            }, "Убедитесь, что метод возвращает объект класса `Response`"

            m.get(url, exc=requests.exceptions.RequestException)

            resp = result.get("/url")

            out, err = capfd.readouterr()
            assert (
                out.strip() == "Возникла ошибка при выполнении запроса"
            ), "Выведите сообщение об ошибке при их возникновении"


class TestSWRequester:
    @pytest.mark.parametrize("kwargs", init_base_url)
    def test_get_sw_info(self, kwargs, sw_type, msg_err):
        assert hasattr(swapi, "SWRequester"), msg_err(
            "add_class", "SWRequester", child=True, parent_name="APIRequester"
        )

        result = swapi.SWRequester(kwargs["base_url"])

        with requests_mock.Mocker() as m:
            url = f'{kwargs["base_url"]}/{sw_type}/'
            m.get(url, text=kwargs["text"])
            try:
                resp = result.get_sw_info(sw_type)
            except requests_mock.exceptions.NoMockAddress:
                assert (
                    m.last_request.url == url
                ), "Убедитесь, что правильно формируете часть адреса из переданных параметров в методе `get_sw_info` класса `SWRequester`"  # noqa

            assert (
                resp == kwargs["text"]
            ), "Убедитесь, что метод `get_sw_info` класса `SWRequester` возвращает ответ в виде строки"  # noqa

    @pytest.mark.parametrize("kwargs", init_base_url)
    def test_get_sw_categories(self, kwargs, sw_type, msg_err):
        result = swapi.SWRequester(kwargs["base_url"])

        with requests_mock.Mocker() as m:
            url = f'{kwargs["base_url"]}/'
            m.get(url, json=kwargs["json"])
            try:
                resp = result.get_sw_categories()
            except requests_mock.exceptions.NoMockAddress:
                assert (
                    m.last_request.url == url
                ), "Убедитесь, что правильно формируете адрес в методе `get_sw_categories` класса `SWRequester`"  # noqa

            assert (
                resp == kwargs["json"].keys()
            ), "Убедитесь, что метод `get_sw_categories` класса `SWRequester` возвращает требуемое значение (ключи словаря)"  # noqa


class MockPath:
    def __init__(self, path) -> None:
        global _path
        _path = path

    def mkdir(self, **kwargs) -> None:
        global _mkdir
        _mkdir = kwargs


class MockSWRequester:
    categories = ["category_1", "category_2", "category_3"]
    text_prefix = "test text for "

    def __init__(self, *args, **kwargs) -> None:
        global _swr_args
        global _swr_kwargs
        _swr_args = args
        _swr_kwargs = kwargs

    def get_sw_categories(self) -> dict:
        return self.categories

    def get_sw_info(self, category) -> str:
        return f"{self.text_prefix}{category}"


_files_ctx = dict()


@contextmanager
def mock_open(*args, **kwargs):
    global _files_ctx, _open_args
    _open_args = args
    pass

    class MockWrite:
        @staticmethod
        def write(data):
            _files_ctx[_open_args[0]] = data

    _f = MockWrite()

    try:
        yield _f
    finally:
        pass


class TestSaveSWData:
    def test_save_sw_data(self):
        _Path = swapi.Path
        swapi.Path = MockPath
        swapi.SWRequester = MockSWRequester
        swapi.open = mock_open

        swapi.save_sw_data()

        swapi.Path = _Path

        assert (
            _path == "data"
        ), "Функция `sawe_sw_data` не создает каталог с именем `data`"
        assert _mkdir == {
            "exist_ok": True
        }, "При создании каталога укажите параметр `exist_ok=True`"

        assert _swr_args == (
            "https://swapi.dev/api",
        ), 'Передайте один параметр `"https://swapi.dev/api"` при объявлении экземпляра класса `SWRequester` в функции `save_sw_data`'  # noqa
        assert (
            _swr_kwargs == {}
        ), 'Передайте один параметр `"https://swapi.dev/api"` при объявлении экземпляра класса `SWRequester` в функции `save_sw_data`'  # noqa

        assert _files_ctx == {
            "data/category_1.txt": "test text for category_1",
            "data/category_2.txt": "test text for category_2",
            "data/category_3.txt": "test text for category_3",
        }, "Убедитесь что функция `save_sw_data` сохраняет результат согласно требования задания"  # noqa
