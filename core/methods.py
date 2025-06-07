# core/methods.py

from typing import List
from database.db_models import TrackedTables
from datetime import timedelta, datetime
from core.config import TIMEZONE
from zoneinfo import ZoneInfo

def filter_by_column(values, col_index, key="TRUE", return_col=0):
    """
    Фильтрует строки, где в колонке col_index значение == key (по умолчанию TRUE),
    и возвращает только колонку return_col.
    """
    result = []
    for row in values:
        if len(row) > col_index and str(row[col_index]).strip().upper() == key:
            if len(row) > return_col:
                result.append([row[return_col]])
    return result

def process_default(values: List[List], source_page_area = None) -> List[List]:
    return values

def process_qa_list(values: List[List], source_page_area = None) -> List[List]:
    return [[row[0]] for row in values[2:] if row and len(row) > 0]

def process_qa_vip_list(values: List[List], source_page_area = None) -> List[List]:
    return filter_by_column(values, col_index=1, key="TRUE", return_col=0)

def process_qa_generic_list(values: List[List], source_page_area = None) -> List[List]:
    return filter_by_column(values, col_index=2, key="TRUE", return_col=0)

def process_qa_legendz_list(values: List[List], source_page_area = None) -> List[List]:
    return filter_by_column(values, col_index=3, key="TRUE", return_col=0)

def process_qa_turkish_list(values: List[List], source_page_area = None) -> List[List]:
    return filter_by_column(values, col_index=4, key="TRUE", return_col=0)


def process_qa_gsbj_list(values: List[List], source_page_area = None) -> List[List]:
    return filter_by_column(values, col_index=5, key="TRUE", return_col=0)


def process_permits(values: List[List], source_page_area = None) -> List[List]:
    result = []
    for row in values:
        if len(row) < 20:
            row += [""] * (20 - len(row))

        col_1 = row[0]
        cols_10_19 = row[9:20] # 19+gsDT1
        result.append([col_1] + cols_10_19)
    return result

def process_rotation(values, source_page_area = None):
    """
    Ищет строку, начинающуюся с "Replacements" в колонке A (index 0),
    и возвращает значения до неё включительно.
    """
    for i, row in enumerate(values):
        if len(row) > 0 and str(row[0]).strip().lower().startswith("replacements"):
            return values[:i + 1]

    raise ValueError("❌ В process_rotation не найдена строка, начинающаяся с 'Replacements'")

def process_shuffle_rotation(values, source_page_area = None):
    """
    Ищет блок между строками "Dealer Name" и "Replacements" в колонке A (index 0).
    """
    start_index = None
    end_index = None

    for i, row in enumerate(values):
        if len(row) > 0:
            cell = str(row[0]).strip().lower()
            if cell == "dealer name" and start_index is None:
                start_index = i + 1
                end_index = start_index + 6

    if start_index is None:
        raise ValueError("❌ В process_shuffle_rotation не найдена строка 'Dealer Name'")

    return values[start_index:end_index]

def process_turkish_rotation(values, source_page_area):
    """
    Ищет блок между строками, содержащими "shift" и "replacements turkish" в колонке A (index 0),
    и добавляет заголовок и Replacements TURKISH с обеих сторон.
    """
    start_index = None
    end_index = None

    for i, row in enumerate(values):
        if len(row) > 0:
            cell = str(row[0]).strip().lower()
            if "shift" in cell and start_index is None:
                start_index = i
            elif "replacements turkish" in cell:
                end_index = i
                break

    if start_index is None:
        raise ValueError("❌ В process_turkish_rotation не найдена строка, содержащая 'shift'")
    if end_index is None:
        raise ValueError("❌ В process_turkish_rotation не найдена строка, содержащая 'replacements turkish'")
    if start_index + 1 > end_index:
        raise ValueError(f"❌ Ошибка в индексах: start={start_index}, end={end_index}")

    # Стандартный блок
    main_block = values[start_index + 1:end_index]

    floor_row = ["TURKISH"] + [""] * 24 + ["HOME"]

    # Выбор строки-заголовка
    if source_page_area and "NIGHT" in source_page_area.upper():
        header_row = ["Dealer Name",
                      "21:00", "21:30", "22:00", "22:30", "23:00", "23:30",
                      "00:00", "00:30", "01:00", "01:30", "02:00", "02:30",
                      "03:00", "03:30", "04:00", "04:30", "05:00", "05:30",
                      "06:00", "06:30", "07:00", "07:30", "08:00", "08:30", "09:00"]
    else:
        header_row = ["Dealer Name",
                      "09:00", "09:30", "10:00", "10:30", "11:00", "11:30",
                      "12:00", "12:30", "13:00", "13:30", "14:00", "14:30",
                      "15:00", "15:30", "16:00", "16:30", "17:00", "17:30",
                      "18:00", "18:30", "19:00", "19:30", "20:00", "20:30", "21:00"]

    # Хвостовая строка Replacements
    replacements_row = ["Replacements TURKISH"] + [""] * 24 + ["HOME"]

    return [floor_row] + [header_row] + main_block + [replacements_row]

PROCESSORS = {
    "process_default": process_default,
    "process_qa_list": process_qa_list,
    "process_qa_vip_list": process_qa_vip_list,
    "process_qa_generic_list": process_qa_generic_list,
    "process_qa_legendz_list": process_qa_legendz_list,
    "process_qa_turkish_list": process_qa_turkish_list,
    "process_qa_gsbj_list": process_qa_gsbj_list,
    "process_qa_tritonrl_list": process_qa_list,
    "process_permits": process_permits,
    "process_rotation": process_rotation,
    "process_shuffle_rotation": process_shuffle_rotation,
    "process_turkish_rotation": process_turkish_rotation
}