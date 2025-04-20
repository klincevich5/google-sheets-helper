from typing import List, Optional, Tuple

# ───────────────────────────────────────────────────────────────
# Основные методы получения диапазонов на основе значений в колонке D
# ───────────────────────────────────────────────────────────────

def get_default_range(page_name: str, explicit_range: str, values: List[List[str]]) -> Optional[str]:
    """
    Возвращает заранее указанную зону без анализа.
    """
    return f"{page_name}!{explicit_range}"


def get_rotation_range(page_name: str, values: List[List[str]]) -> Optional[str]:
    """
    Ищет в колонке D (index 3) ячейку с текстом 'Rotations'.
    Возвращает диапазон D1:ACx, где x — строка, в которой найдено слово.
    """
    for idx, row in enumerate(values, start=1):
        if len(row) >= 4 and row[3].strip().lower() == "rotations":
            return f"{page_name}!D1:AC{idx}"
    return None  # ❌ Не найдено слово "Rotations"


def get_turkish_rotation_range(page_name: str, values: List[List[str]]) -> Optional[str]:
    """
    Ищет 'shift:' и 'Replacements TURKISH' в колонке D.
    Возвращает диапазон от строки после shift: до строки с Replacements TURKISH.
    """
    x, y = None, None
    for idx, row in enumerate(values, start=1):
        cell = row[3].strip().lower() if len(row) >= 4 else ""
        if "shift:" in cell and x is None:
            x = idx
        if "replacements turkish" in cell and y is None:
            y = idx
    if x and y and y > x:
        return f"{page_name}!D{x+1}:AC{y}"
    return None  # ❌ Ошибка в определении диапазона


def get_shuffle_rotation_range(page_name: str, values: List[List[str]]) -> Optional[str]:
    """
    Ищет 'Dealer Name' и 'Replacements' в колонке D.
    Возвращает диапазон от строки после Dealer Name до строки с Replacements.
    """
    x, y = None, None
    for idx, row in enumerate(values, start=1):
        cell = row[3].strip().lower() if len(row) >= 4 else ""
        if "dealer name" in cell and x is None:
            x = idx
        if "replacements" == cell and y is None:
            y = idx
    if x and y and y > x:
        return f"{page_name}!D{x+1}:AC{y}"
    return None  # ❌ Ошибка в определении диапазона


# ───────────────────────────────────────────────────────────────
# Универсальный диспетчер метода
# ───────────────────────────────────────────────────────────────

def get_range_by_method(method_name: str, page_name: str, area: str, values: List[List[str]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Унифицированный диспетчер методов получения диапазонов.
    Возвращает (диапазон, ошибка) — если диапазон определить не удалось, в error будет причина.
    """
    if method_name == "get_default":
        return get_default_range(page_name, area, values), None
    elif method_name == "get_rotation":
        result = get_rotation_range(page_name, values)
        return result, None if result else "❌ Не найдено слово 'Rotations' в колонке D"
    elif method_name == "get_turkish_rotation":
        result = get_turkish_rotation_range(page_name, values)
        return result, None if result else "❌ Не найдено 'shift:' или 'Replacements TURKISH' в колонке D"
    elif method_name == "get_shuffle_rotation":
        result = get_shuffle_rotation_range(page_name, values)
        return result, None if result else "❌ Не найдено 'Dealer Name' или 'Replacements' в колонке D"
    else:
        return None, f"❌ Неизвестный метод: {method_name}"
