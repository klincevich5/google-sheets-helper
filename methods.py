# methods.py

from typing import List, Optional, Tuple
from logger import log_to_file
from config import SHEETS_LOG_FILE


def filter_by_column(values, col_index, key="TRUE"):
    return [[row[0]] for row in values if len(row) > col_index and str(row[col_index]).strip().upper() == key and len(row) > 0]

def process_default(values):
    return values

def process_QA_VIP_List(values):
    return filter_by_column(values, 1)

def process_QA_GENERIC_List(values):
    return filter_by_column(values, 2)

def process_QA_LEGENDZ_List(values):
    return filter_by_column(values, 3)

def process_QA_TURKISH_List(values):
    return filter_by_column(values, 4)

def process_QA_GSBJ_List(values):
    return filter_by_column(values, 5)

def process_QA_List(values):
    return [[row[0]] for row in values if len(row) > 0]

def process_Permits(values):
    result = []
    for row in values:
        if len(row) < 21:  # Убедимся, что строка имеет достаточное количество колонок
            row += [""] * (21 - len(row))  # Дополняем пустыми значениями, если их не хватает

        col_1 = row[0]  # Первая колонка
        cols_10_18 = row[9:18]  # Колонки с 10 по 18
        is_true = any(str(c).strip().upper() == "TRUE" for c in row[18:21])  # Проверяем значения в колонках 19-21
        combined_status = "TRUE" if is_true else "FALSE"  # Логика объединения

        # Формируем строку результата
        result.append([col_1] + cols_10_18 + [combined_status])

    return result

def proc_func(method, values):
    if method == "process_default":
        values = process_default(values)
    elif method == "process_QA_VIP_List":
        values = process_QA_VIP_List(values)
    elif method == "process_QA_GENERIC_List":
        values = process_QA_GENERIC_List(values)
    elif method == "process_QA_LEGENDZ_List":
        values = process_QA_LEGENDZ_List(values)
    elif method == "process_QA_TURKISH_List":
        values = process_QA_TURKISH_List(values)
    elif method == "process_QA_GSBJ_List":
        values = process_QA_GSBJ_List(values)
    elif method == "process_QA_List":
        values = process_QA_List(values)
    elif method == "process_Permits":
        values = process_Permits(values)
    return values
