# core/methods.py

from typing import List

import re

def normalize_cell(cell):
    """
    Удаляет невидимые символы (\u200B, \u00A0), заменяет множественные пробелы на один,
    обрезает по краям и приводит к нижнему регистру.
    """
    if cell is None:
        return ""
    cleaned = re.sub(r'[\u200B\u00A0]', '', str(cell).strip())
    collapsed = re.sub(r'\s+', ' ', cleaned)
    return collapsed.strip()

def process_single_column_list(values: List[List]) -> List[dict]:
    """
    Преобразует список списков вида [["Aidana Tokhdaulet"], [], ["Aksinya Laptsenak"]]
    в jsonb-совместимый список словарей: [{"name": "Aidana Tokhdaulet"}, ...]
    Пустые строки пропускаются.
    """
    result = []
    for row in values:
        if row and len(row) > 0:
            name = normalize_cell(row[0])
            if name:
                result.append({"name": name})
    return result

def filter_by_column(values, col_index, key="TRUE", return_col=0):
    """
    Фильтрует строки по значению в колонке col_index == key.
    """
    result = []
    for row in values:
        if len(row) > col_index and normalize_cell(row[col_index]).upper() == key:
            if len(row) > return_col:
                result.append([normalize_cell(row[return_col])])
    return result


def process_default(values: List[List], source_page_area = None) -> List[List]:
    return process_single_column_list(values)

def process_qa_list(values: List[List], source_page_area=None) -> List[dict]:
    filtered = [[row[0]] for row in values[2:] if row and len(row) > 0]
    return process_single_column_list(filtered)

def process_qa_vip_list(values: List[List], source_page_area=None) -> List[dict]:
    filtered = filter_by_column(values, col_index=1, key="TRUE", return_col=0)
    return process_single_column_list(filtered)

def process_qa_generic_list(values: List[List], source_page_area=None) -> List[dict]:
    filtered = filter_by_column(values, col_index=2, key="TRUE", return_col=0)
    return process_single_column_list(filtered)

def process_qa_legendz_list(values: List[List], source_page_area=None) -> List[dict]:
    filtered = filter_by_column(values, col_index=3, key="TRUE", return_col=0)
    return process_single_column_list(filtered)

def process_qa_turkish_list(values: List[List], source_page_area=None) -> List[dict]:
    filtered = filter_by_column(values, col_index=4, key="TRUE", return_col=0)
    return process_single_column_list(filtered)

def process_qa_gsbj_list(values: List[List], source_page_area=None) -> List[dict]:
    filtered = filter_by_column(values, col_index=5, key="TRUE", return_col=0)
    return process_single_column_list(filtered)

def process_feedbacks_status(values: List[List], source_page_area=None) -> List[dict]:
    if not values:
        return []

    default_header = [
        "Nr", "Date", "Shift", "Floor", "Game", "GP Name Surname", "SM Name Surname",
        "Reason", "Total", "Proof", "Explanation of the reason", "Action taken", "Forwarded Feedback", "Comment after forwarding",
    ]

    first_row = values[0]
    is_data_row = any(
        isinstance(cell, str) and cell.strip().count('.') == 2 for cell in first_row
    )

    if is_data_row:
        header = default_header
        data_rows = values
    else:
        header = [normalize_cell(h) for h in values[0]]
        data_rows = values[1:]

    result = []
    for i, row in enumerate(data_rows, start=2):
        if not any(row):
            continue

        row_dict = {}
        for col_index, col_value in enumerate(row):
            if col_index >= len(header):
                continue
            key = normalize_cell(header[col_index])
            value = normalize_cell(col_value)
            if key:
                row_dict[key] = value

        if not row_dict.get("Date") or not row_dict.get("Shift"):
            continue

        result.append(row_dict)

    return result

def process_feedbacks(values: List[List], source_page_area=None) -> List[dict]:
    if len(values) < 2:
        return []

    header = [normalize_cell(h) for h in values[1]]
    data_rows = values[2:]

    result = []
    for row in data_rows:
        if any(normalize_cell(cell) for cell in row):
            item = {
                header[i]: normalize_cell(row[i]) if i < len(row) else ""
                for i in range(len(header))
            }
            result.append(item)

    return result

def process_mistake_in_db(values: List[List], source_page_area=None) -> List[dict]:
    if len(values) < 2:
        return []

    header = [normalize_cell(h) for h in values[0]]
    data_rows = values[1:]

    result = []
    for row in data_rows:
        if any(normalize_cell(cell) for cell in row):
            entry = {
                header[i]: normalize_cell(row[i]) if i < len(row) else ""
                for i in range(len(header))
            }
            result.append(entry)

    return result

def process_permits(values: List[List], source_page_area=None) -> List[dict]:
    if len(values) < 3:
        return []

    raw_header = values[0]

    exclude = {
        "VIP", "GENERIC", "LegendZ", "TURKISH", "GSBJ", "TRISTAR", "TritonRL",
        "Quality Control Manager's Note"
    }

    header_pairs = [
        (i, "name" if normalize_cell(h).lower().startswith("dealer name") else normalize_cell(h))
        for i, h in enumerate(raw_header)
        if normalize_cell(h) not in exclude
    ]

    result = []
    for row in values[2:]:
        if not row or all((not normalize_cell(cell)) for cell in row):
            continue

        if not normalize_cell(row[0]):
            continue

        entry = {
            name: normalize_cell(row[i]) if i < len(row) else ""
            for i, name in header_pairs
        }
        result.append(entry)

    return result



def process_qa_list_in_db(values: List[List], source_page_area=None) -> List[dict]:
    if len(values) < 3:
        return []

    raw_header = values[0]
    data_rows = values[2:]

    header = [
        "name" if normalize_cell(h).lower().startswith("dealer name") else normalize_cell(h)
        for h in raw_header
    ]

    result = []
    for row in data_rows:
        if not row or all((not normalize_cell(cell)) for cell in row):
            continue

        if not normalize_cell(row[0]):
            continue

        entry = {
            header[i]: normalize_cell(row[i]) if i < len(row) else ""
            for i in range(len(header))
        }
        result.append(entry)

    return result

def process_schedule_OT_json(values: List[List], source_page_area=None) -> List[dict]:
    result = []
    for row in values:
        if not row or all((not normalize_cell(cell)) for cell in row):
            continue

        if len(row) < 1 or not normalize_cell(row[0]):
            continue

        dealer_name = normalize_cell(row[0])
        item = {"dealer_name": dealer_name}

        for i in range(1, len(row)):
            shift = normalize_cell(row[i])
            item[f"day_{i}"] = shift

        result.append(item)

    return result

def process_sm_schedule(values: List[List], source_page_area=None) -> List[dict]:
    if len(values) < 3:
        return []

    day_labels = [normalize_cell(d) for d in values[0]]
    data_rows = values[2:]

    result = []
    for row in data_rows:
        if not row or all((not normalize_cell(cell)) for cell in row):
            continue

        if not normalize_cell(row[0]):
            continue

        sm_name = normalize_cell(row[0])
        entry = {"sm_name": sm_name}

        for col_idx in range(1, min(len(row), len(day_labels))):
            day_str = normalize_cell(day_labels[col_idx])
            if not day_str.isdigit():
                continue

            day_key = f"day_{int(day_str)}"
            shift = normalize_cell(row[col_idx])
            entry[day_key] = shift

        result.append(entry)

    return result























def process_rotation(values, source_page_area=None):
    try:
        for i, row in enumerate(values):
            if len(row) > 0 and normalize_cell(row[0]).lower().startswith("replacements"):
                return values[:i + 1]
        raise ValueError("❌ В process_rotation не найдена строка, начинающаяся с 'Replacements'")
    except Exception as e:
        import logging
        logging.error(f"[process_rotation] Ошибка: {e}")
        return values

def process_full_rotation(values, source_page_area=None):
    try:
        for i, row in enumerate(values):
            if len(row) > 0 and "shift" in normalize_cell(row[0]).lower():
                return values[:i + 1]
        raise ValueError("❌ В process_full_rotation не найдена строка, содержащая 'shift'")
    except Exception as e:
        import logging
        logging.error(f"[process_full_rotation] Ошибка: {e}")
        return values

def process_shuffle_rotation(values, source_page_area=None):
    try:
        start_index = None
        for i, row in enumerate(values):
            if len(row) > 0:
                cell = normalize_cell(row[0]).lower()
                if cell == "dealer name" and start_index is None:
                    start_index = i + 1
                    end_index = start_index + 6
        if start_index is None:
            raise ValueError("❌ В process_shuffle_rotation не найдена строка 'Dealer Name'")
        return values[start_index:end_index]
    except Exception as e:
        import logging
        logging.error(f"[process_shuffle_rotation] Ошибка: {e}")
        return values

def process_turkish_rotation(values, source_page_area=None):
    try:
        start_index = None
        end_index = None
        for i, row in enumerate(values):
            if len(row) > 0:
                cell = normalize_cell(row[0]).lower()
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

        main_block = values[start_index + 1:end_index]

        floor_row = ["TURKISH"] + [""] * 24 + ["HOME"]

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

        replacements_row = ["Replacements TURKISH"] + [""] * 24 + ["HOME"]

        return [floor_row] + [header_row] + main_block + [replacements_row]
    except Exception as e:
        import logging
        logging.error(f"[process_turkish_rotation] Ошибка: {e}")
        return values

def process_full_turkish_rotation(values, source_page_area=None):
    try:
        start_index = None
        end_index = None
        shift_count = 0
        for i, row in enumerate(values):
            if len(row) > 0:
                cell = normalize_cell(row[0]).lower()
                if "shift" in cell:
                    shift_count += 1
                    if shift_count == 1:
                        start_index = i
                    elif shift_count == 2:
                        end_index = i
                        break
        if start_index is None:
            raise ValueError("❌ В process_full_turkish_rotation не найдена первая строка, содержащая 'shift'")
        if end_index is None:
            raise ValueError("❌ В process_full_turkish_rotation не найдена вторая строка, содержащая 'shift'")
        if start_index + 1 > end_index:
            raise ValueError(f"❌ Ошибка в индексах: start={start_index}, end={end_index}")

        main_block = values[start_index + 1:end_index]
        floor_row = ["TURKISH"] + [""] * 24 + ["HOME"]
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

        replacements_row = ["Replacements TURKISH"] + [""] * 24 + ["HOME"]
        return [floor_row] + [header_row] + main_block + [replacements_row]
    except Exception as e:
        import logging
        logging.error(f"[process_full_turkish_rotation] Ошибка: {e}")
        return values

PROCESSORS = {
    "process_schedule_OT_json": process_schedule_OT_json,
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
    "process_full_rotation": process_full_rotation,
    "process_shuffle_rotation": process_shuffle_rotation,
    "process_turkish_rotation": process_turkish_rotation,
    "process_full_turkish_rotation": process_full_turkish_rotation,
    "process_mistake_in_db": process_mistake_in_db,
    "process_feedbacks_status": process_feedbacks_status,
    "process_feedbacks": process_feedbacks,
    "process_qa_list_in_db": process_qa_list_in_db,
    "process_sm_schedule": process_sm_schedule
}