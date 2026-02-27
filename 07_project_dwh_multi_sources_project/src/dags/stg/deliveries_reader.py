import requests  
from typing import Dict, List, Optional  
from datetime import datetime


class DeliveryReader:
    def __init__(self, api_url: str, api_headers: Dict):
        self.api_url = api_url  
        self.api_headers = api_headers

    def get_deliveries(
        self,
        offset: int,
        limit: int,
        sort_field: str = 'id',  
        sort_direction: str = 'asc',
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None  
    ) -> List[Dict]:
        """
        Получает список доставок с API с учетом пагинации, сортировки и фильтрации по дате
        
        Args:
            offset: Смещение для постраничной загрузки  
            limit: Количество записей на странице  
            sort_field: Поле для сортировки (по умолчанию 'id')
            sort_direction: Направление сортировки ('asc' или 'desc')
            start_date: Начальная дата для фильтрации (включительно)
            end_date: Конечная дата для фильтрации (включительно)
            
        Returns:
            Список словарей с данными о доставках  
        """
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset  
        }

        # Добавляем фильтрацию по дате, если параметры переданы  
        if start_date is not None:
            params['date_from'] = start_date.isoformat()
        if end_date is not None:
            params['date_to'] = end_date.isoformat()

        try:
            response = requests.get(
                f"{self.api_url}/deliveries",
                headers=self.api_headers,
                params=params,
                timeout=10  # Таймаут на запрос (секунды)
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch deliveries: {str(e)}")
