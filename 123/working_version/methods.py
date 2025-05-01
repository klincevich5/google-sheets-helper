from typing import List

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

def process_default(values: List[List]) -> List[List]:
    return values

def process_qa_list(values: List[List]) -> List[List]:
    return [[row[0]] for row in values[2:] if row and len(row) > 0]

def process_qa_vip_list(values: List[List]) -> List[List]:
    return filter_by_column(values, col_index=1, key="TRUE", return_col=0)

def process_qa_generic_list(values: List[List]) -> List[List]:
    return filter_by_column(values, col_index=2, key="TRUE", return_col=0)

def process_qa_legendz_list(values: List[List]) -> List[List]:
    return filter_by_column(values, col_index=3, key="TRUE", return_col=0)

def process_qa_turkish_list(values: List[List]) -> List[List]:
    return filter_by_column(values, col_index=4, key="TRUE", return_col=0)


def process_qa_gsbj_list(values: List[List]) -> List[List]:
    return filter_by_column(values, col_index=5, key="TRUE", return_col=0)


def process_permits(values: List[List]) -> List[List]:
    result = []
    for row in values:
        if len(row) < 20:
            row += [""] * (20 - len(row))

        col_1 = row[0]
        cols_10_20 = row[9:19]
        result.append([col_1] + cols_10_20)
    return result

def process_rotation(values):
    """
    Ищет строку, начинающуюся с "Replacements" в колонке A (index 0),
    и возвращает значения до неё включительно.
    """
    for i, row in enumerate(values):
        if len(row) > 0 and str(row[0]).strip().lower().startswith("replacements"):
            return values[:i + 1]

    raise ValueError("❌ В process_rotation не найдена строка, начинающаяся с 'Replacements'")

def process_shuffle_rotation(values):
    """
    Ищет блок между строками "Dealer Name" и "Replacements" в колонке A (index 0).
    """
    start_index = None
    end_index = None

    for i, row in enumerate(values):
        if len(row) > 0:
            cell = str(row[0]).strip().lower()
            if cell == "dealer name" and start_index is None:
                start_index = i
            elif cell.startswith("replacements"):
                end_index = i
                break

    if start_index is None:
        raise ValueError("❌ В process_shuffle_rotation не найдена строка 'Dealer Name'")
    if end_index is None:
        raise ValueError("❌ В process_shuffle_rotation не найдена строка, начинающаяся с 'Replacements'")
    if start_index + 1 > end_index:
        raise ValueError(f"❌ Ошибка в индексах: start={start_index}, end={end_index}")

    return values[start_index + 1:end_index]


def process_turkish_rotation(values):
    """
    Ищет блок между строками, содержащими "shift" и "replacements turkish" в колонке A (index 0).
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

    return values[start_index + 1:end_index]

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