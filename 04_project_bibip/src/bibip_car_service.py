import os
from models import Car, CarFullInfo, CarStatus, Model, ModelSaleStats, Sale
from datetime import datetime
from decimal import Decimal


class CarService:
    def __init__(self, root_directory_path: str) -> None:
        self.root_directory_path = root_directory_path

    # Задание 1. Сохранение автомобилей и моделей
    def add_model(self, model: Model) -> Model:
        """Добавляет модели в базу данных. При добавлении запись появляется сразу в двух файлах."""

        info = f"{model.id}, {model.name}, {model.brand}\n"
        path_models = f"{self.root_directory_path}/models.txt"
        with open(path_models, "a", encoding="utf-8") as f:
            f.write(info)
        line_number = sum(1 for _ in open(path_models, "r", encoding="utf-8"))
        note = f"{model.name}, {line_number}\n"
        path_models_index = f"{self.root_directory_path}/models_index.txt"
        with open(path_models_index, "a", encoding="utf-8") as file:
            file.write(note)

    # Задание 1. Сохранение автомобилей и моделей
    def add_car(self, car: Car) -> Car:
        """Добавляет автомобили в базу данных. При добавлении запись появляется сразу в двух файлах."""

        info = f"{car.vin}, {car.model}, {car.price}, {car.date_start}, {car.status}\n"
        path_cars = f"{self.root_directory_path}/cars.txt"
        with open(path_cars, "a", encoding="utf-8") as f:
            f.write(info)
        line_number = sum(1 for _ in open(path_cars, "r", encoding="utf-8"))
        note = f"{car.vin}, {line_number}\n"
        path_cars_index = f"{self.root_directory_path}/cars_index.txt"
        with open(path_cars_index, "a", encoding="utf-8") as file:
            file.write(note)

    # Задание 2. Сохранение продаж.
    def sell_car(self, sale: Sale) -> Car:
        """Добавляет новую продажу в базу данных."""

        path_sales = f"{self.root_directory_path}/sales.txt"
        path_sales_index = f"{self.root_directory_path}/sales_index.txt"
        path_cars_index = f"{self.root_directory_path}/cars_index.txt"
        path_cars = f"{self.root_directory_path}/cars.txt"

        with open(path_sales, "a", encoding="utf-8") as f:
            f.write(
                f"{sale.sales_number}, {sale.car_vin}, {sale.sales_date}, {sale.cost}\n"
            )

        with open(path_sales_index, "a", encoding="utf-8") as f:
            line_number = sum(1 for _ in open(path_sales, "r", encoding="utf-8"))
            note = f"{sale.sales_number}, {line_number}\n"
            f.write(note)

        with open(path_sales_index, "r", encoding="utf-8") as f:
            path_sales_index_lines = f.readlines()
            row_number = path_sales_index_lines[0].split(",")
            row_val = int(row_number[1].strip()) - 1

        with open(path_sales, "r", encoding="utf-8") as f:
            path_sales_lines = f.readlines()
            parts = path_sales_lines[row_val].strip().split(",")
            vin = parts[1].strip()

        with open(path_cars_index, "r", encoding="utf-8") as f:
            path_cars_index_lines = f.readlines()
            for i in path_cars_index_lines:
                parts = i.strip().split(",")
                current_vin = parts[0].strip()
                if vin == current_vin:
                    row_number = int(parts[1].strip()) - 1

        with open(path_cars, "r+", encoding="utf-8") as f:
            path_cars_lines = f.readlines()
            parts = path_cars_lines[row_number].strip().split(",")
            parts[-1] = "sold"
            path_cars_lines[row_number] = ",".join(parts) + "\n"
            f.seek(0)
            f.writelines(path_cars_lines)
            f.truncate()

    # Задание 3. Доступные к продаже
    def get_cars(self, status: CarStatus) -> list[Car]:
        """Выводит список моделей, доступных к покупке прямо сейчас."""

        available_cars = []
        path_cars = f"{self.root_directory_path}/cars.txt"
        with open(path_cars, "r", encoding="utf-8") as f:
            str_from_models = f.readlines()

            for i in str_from_models:
                if i.split()[-1] == "available":
                    available_cars.append(i[:-1])

        available_cars_final = []
        for available_car in available_cars:
            car_data = {
                "vin": available_car.split(",")[0],
                "model": available_car.split(",")[1],
                "price": available_car.split(",")[2],
                "date_start": available_car.split(",")[3].strip(),
                "status": CarStatus(available_car.split(",")[4].strip()),
            }
            available_cars_final.append(Car(**car_data))
        return available_cars_final

    # Задание 4. Детальная информация
    def get_car_info(self, vin: str) -> CarFullInfo | None:
        """Выводит информацию о машине по VIN-коду."""

        path_cars_index = f"{self.root_directory_path}/cars_index.txt"
        path_cars = f"{self.root_directory_path}/cars.txt"
        path_models = f"{self.root_directory_path}/models.txt"
        path_sales = f"{self.root_directory_path}/sales.txt"

        with open(path_cars_index, "r", encoding="utf-8") as f:
            path_cars_index_lines = f.readlines()
            for i in path_cars_index_lines:
                parts = i.strip().split(",")
                if vin == parts[0]:
                    row_index_car = int(parts[1])
                    break
            else:
                return None

        with open(path_cars, "r", encoding="utf-8") as f_cars, open(
            path_models, "r", encoding="utf-8"
        ) as f_model:

            f_model_lines = f_model.readlines()
            f_cars_lines = f_cars.readlines()
            search_parts_f_cars = f_cars_lines[row_index_car - 1].strip().split(",")
            key_model = int(search_parts_f_cars[1])
            status_part = search_parts_f_cars[-1].strip()
            search_parts_f_model = f_model_lines[key_model - 1]

            if status_part != "sold":
                join_info_sold = ",".join(
                    search_parts_f_cars + [search_parts_f_model]
                ).split(",")
                available_data = CarFullInfo(
                    vin=join_info_sold[0].strip(),
                    car_model_name=join_info_sold[6].strip(),
                    car_model_brand=join_info_sold[7].strip(),
                    price=join_info_sold[2].strip(),
                    date_start=join_info_sold[3].strip(),
                    status=CarStatus(join_info_sold[4].strip()),
                    sales_date=None,
                    sales_cost=None,
                )
                return available_data
            else:
                with open(path_sales, "r", encoding="utf-8") as f_sales:
                    f_sales_lines = f_sales.readlines()
                    join_info_sold = ",".join(
                        search_parts_f_cars + [search_parts_f_model]
                    ).split(",")
                    sold_raw_data = ",".join(join_info_sold + f_sales_lines).split(",")
                    sold_data = CarFullInfo(
                        vin=sold_raw_data[0].strip(),
                        car_model_name=sold_raw_data[6].strip(),
                        car_model_brand=sold_raw_data[7].strip(),
                        price=sold_raw_data[2].strip(),
                        date_start=sold_raw_data[3].strip(),
                        status=CarStatus(sold_raw_data[4].strip()),
                        sales_date=sold_raw_data[-2].strip(),
                        sales_cost=sold_raw_data[-1].strip(),
                    )
                    return sold_data

    # Задание 5. Обновление ключевого поля
    def update_vin(self, vin: str, new_vin: str) -> Car:
        """Исправляет VIN-код на корректный."""

        path_cars_index = f"{self.root_directory_path}/cars_index.txt"
        path_cars = f"{self.root_directory_path}/cars.txt"

        with open(path_cars_index, "r", encoding="utf-8") as f:
            cars_index_lines = f.readlines()
            for i in cars_index_lines:
                if vin in i:
                    num_string = i[-2]

        with open(path_cars, "r+", encoding="utf-8") as f:
            cars_lines = f.readlines()
            cars_lines_need_str = (
                cars_lines[int(num_string) - 1].strip().split(",")
            )  # b. Найдите строку в cars.txt по номеру строки из car_index.txt.
            cars_lines_need_str[0] = new_vin
            str_cars_lines_need_str = ",".join(cars_lines_need_str) + "\n"
            cars_lines[int(num_string) - 1] = str_cars_lines_need_str
            f.seek(0)
            f.writelines(cars_lines)

        with open(path_cars_index, "r+", encoding="utf-8") as f:
            lines = f.readlines()
            note = lines[int(num_string) - 1].strip().split(",")
            note[0] = new_vin
            new_note = ",".join(note) + "\n"
            lines[int(num_string) - 1] = new_note
            f.seek(0)
            f.writelines(lines)

    # Задание 6. Удаление продажи
    def revert_sale(self, sales_number: str) -> Car:
        """Удаляет запись из таблицы продаж и заменяет статус для машины в таблице Cars."""

        path_sales_index = f"{self.root_directory_path}/sales_index.txt"

        with open(path_sales_index, "r", encoding="utf-8") as f:
            sales_index_string = f.readlines()
            for i in sales_index_string:
                if sales_number in i:
                    num_string = i.split(",")[1].strip()
                    num_string_int = int(num_string)

        path_sales = f"{self.root_directory_path}/sales.txt"

        with open(path_sales, "r+", encoding="utf-8") as f:
            strings_sales = f.readlines()
            del strings_sales[num_string_int - 1]

        with open(path_sales, "w", encoding="utf-8") as f:
            f.writelines(strings_sales)

        path_cars_index = f"{self.root_directory_path}/cars_index.txt"

        with open(path_cars_index, "r", encoding="utf-8") as f:
            cars_index_lines = f.readlines()
            for i in cars_index_lines:
                if sales_number.split("#")[1] in i:
                    num_string = i[-2]

        path_cars = f"{self.root_directory_path}/cars.txt"

        with open(path_cars, "r+", encoding="utf-8") as f:
            cars_lines = f.readlines()
            note = cars_lines[int(num_string) - 1].strip().split(",")
            note[-1] = " available"
            new_note = ",".join(note) + "\n"
            cars_lines[int(num_string) - 1] = new_note
            f.seek(0)
            f.writelines(cars_lines)

    # Задание 7. Самые продаваемые модели
    def top_models_by_sales(self) -> list[ModelSaleStats]:
        """Выводит топ-3 моделей по количеству продаж в автосалоне."""

        path_cars = f"{self.root_directory_path}/cars.txt"
        path_models_index = f"{self.root_directory_path}/models_index.txt"
        path_models = f"{self.root_directory_path}/models.txt"
        path_sales = f"{self.root_directory_path}/sales.txt"

        sales_stats: dict[int, int] = {}
        vin_to_model: dict[str, int] = {}

        with open(path_cars, "r", encoding="utf-8") as cars_file:
            for line in cars_file:
                parts = line.strip().split(",")
                vin_to_model[parts[0]] = int(parts[1])

        with open(path_sales, "r", encoding="utf-8") as sales_file:
            for line in sales_file:
                parts = line.strip().split(",")
                vin = parts[1].strip()
                if vin in vin_to_model:
                    model_id = vin_to_model[vin]
                    sales_stats[model_id] = sales_stats.get(model_id, 0) + 1

        model_prices: dict[int, Decimal] = {}

        with open(path_cars, "r", encoding="utf-8") as cars_file:
            for line in cars_file:
                parts = line.strip().split(",")
                model_id = int(parts[1])
                price = Decimal(parts[2])
                if model_id not in model_prices or price > model_prices[model_id]:
                    model_prices[model_id] = price

        sorted_models: list[int] = sorted(
            sales_stats.keys(),
            key=lambda x: (-sales_stats[x], -model_prices.get(x, Decimal(0))),
        )[:3]

        result = []
        model_index = {}

        if os.path.exists(path_models_index):
            with open(path_models_index, "r", encoding="utf-8") as index_file:
                for line in index_file:
                    parts = line.strip().split(",")
                    model_index[parts[0]] = int(parts[1])

        with open(path_models, "r", encoding="utf-8") as models_file:
            models_data = models_file.readlines()
        for model_id in sorted_models:
            model_data = None
            if model_data is None:
                for line in models_data:
                    parts = line.strip().split(",")
                    if len(parts) >= 3 and int(parts[0]) == model_id:
                        model_data = line.strip()
                        break
                parts = model_data.split(",")
                if len(parts) >= 3:
                    result.append(
                        ModelSaleStats(
                            car_model_name=parts[1].strip(),
                            brand=parts[2].strip(),
                            sales_number=sales_stats[model_id],
                        )
                    )
        return result
