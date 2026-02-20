import requests
from typing import Dict, List, Optional
from datetime import datetime


class CourierReader:
    def __init__(self, api_url: str, api_headers: Dict):
        self.api_url = api_url
        self.api_headers = api_headers

    def get_couriers(
        self,
        offset: int,
        limit: int,
        sort_field: str = '_id',
        sort_direction: str = 'asc',
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Получает список курьеров с API с поддержкой пагинации, сортировки и фильтрации по дате
        
        Args:
            offset: Смещение для постраничной загрузки
            limit: Количество записей на странице
            sort_field: Поле для сортировки (по умолчанию '_id')
            sort_direction: Направление сортировки ('asc' или 'desc')
            date_from: Начальная дата фильтрации (включительно)
            date_to: Конечная дата фильтрации (включительно)
            
        Returns:
            Список словарей с данными о курьерах
        """
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }

        # Добавляем параметры дат, если они указаны
        if date_from is not None:
            params['date_from'] = date_from.isoformat()
        if date_to is not None:
            params['date_to'] = date_to.isoformat()

        try:
            response = requests.get(
                f"{self.api_url}/couriers",
                headers=self.api_headers,
                params=params,
                timeout=10  # Таймаут на запрос в секундах
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch couriers: {str(e)}")

