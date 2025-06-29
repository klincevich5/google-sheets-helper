# utils/formatting_utils.py

import time
import socket
# from utils.db_orm import insert_usage
from utils.logger import (
    log_info, log_success, log_warning, log_error, log_section, log_separator
)

# 🎨 Цветовая карта по ключевым словам
COLOR_MAP = {
    "SC": {"fg": "#000000", "bg": "#00ffff"},
    "TC": {"fg": "#000000", "bg": "#8179c7"},
    "FC": {"fg": "#000000", "bg": "#e6cd74"},
    "HOME": {"fg": "#000000", "bg": "#00ffff"},
    "FLOOR": {"fg": "#ffffff", "bg": "#11499e"},
    "LATE": {"fg": "#000000", "bg": "#ff0000"},
    "VIP": {"fg": "#000000", "bg": "#ffff00"},
    "TURKISH": {"fg": "#000000", "bg": "#f11d52"},
    "GENERIC": {"fg": "#000000", "bg": "#ff9900"},
    "GSBJ": {"fg": "#000000", "bg": "#a64d79"},
    "LEGENDZ": {"fg": "#000000", "bg": "#34a853"},
    "TRISTAR": {"fg": "#b77a30", "bg": "#434343"},
    "TRITONRL": {"fg": "#ffff00", "bg": "#073763"},
    "VIP/GEN": {"fg": "#000000", "bg": "#ff9900"},
    "LZ/GS": {"fg": "#000000", "bg": "#1609c7"},
    "GEN": {"fg": "#000000", "bg": "#ff9900"},
    "LZ": {"fg": "#000000", "bg": "#34a853"},
    "vBJ2": {"fg": "#000000", "bg": "#e6cd74"},
    "vBJ3": {"fg": "#000000", "bg": "#21cbab"},
    "gBC1": {"fg": "#000000", "bg": "#d5a6bd"},
    "vBC3": {"fg": "#000000", "bg": "#a160f3"},
    "vBC4": {"fg": "#000000", "bg": "#e06666"},
    "vHSB1": {"fg": "#000000", "bg": "#ff50e8"},
    "vDT1": {"fg": "#000000", "bg": "#e91a1a"},
    "gsRL1": {"fg": "#e5cff2", "bg": "#5a3286"},
    "swBC1": {"fg": "#ffffff", "bg": "#11734b"},
    "swRL1": {"fg": "#000000", "bg": "#ffff00"},
    "tBJ1": {"fg": "#ffffff", "bg": "#6633cc"},
    "tBJ2": {"fg": "#000000", "bg": "#3d86f8"},
    "tRL1": {"fg": "#000000", "bg": "#f11d52"},
    "gBJ1": {"fg": "#000000", "bg": "#00ffff"},
    "gBJ3": {"fg": "#000000", "bg": "#ffe599"},
    "gBJ4": {"fg": "#000000", "bg": "#a64d79"},
    "gBJ5": {"fg": "#000000", "bg": "#cc0000"},
    "gBC2": {"fg": "#000000", "bg": "#fbbc04"},
    "gBC3": {"fg": "#000000", "bg": "#3c78d8"},
    "gBC4": {"fg": "#000000", "bg": "#e69138"},
    "gBC5": {"fg": "#000000", "bg": "#ffff00"},
    "gBC6": {"fg": "#000000", "bg": "#6aa84f"},
    "gRL1": {"fg": "#000000", "bg": "#ff6d01"},
    "gRL2": {"fg": "#ffcfc9", "bg": "#b10202"},
    "gsBJ1": {"fg": "#000000", "bg": "#00ffff"},
    "gsBJ2": {"fg": "#000000", "bg": "#ff9900"},
    "gsBJ3": {"fg": "#000000", "bg": "#ffe599"},
    "gsDT1": {"fg": "#000000", "bg": "#ffe599"},
    "gsBJ4": {"fg": "#000000", "bg": "#a64d79"},
    "gsBJ5": {"fg": "#000000", "bg": "#cc0000"},
    "gRL3": {"fg": "#ffffff", "bg": "#b45f06"},
    "lBJ1": {"fg": "#000000", "bg": "#e75c74"},
    "lBJ2": {"fg": "#000000", "bg": "#54a8b2"},
    "lBJ3": {"fg": "#000000", "bg": "#ffe5a0"},
    "AB": {"fg": "#d4edbc", "bg": "#11734b"},
    "L7": {"fg": "#ffffff", "bg": "#2b9de8"},
    "DT": {"fg": "#bfe0f6", "bg": "#0a53a8"},
    "TP": {"fg": "#ffcfc9", "bg": "#b10202"},
    "DTL": {"fg": "#e5cff2", "bg": "#5a3286"},
    "TritonRL": {"fg": "#bfe0f6", "bg": "#0a53a8"},
    "Shuffle": {"fg": "#000000", "bg": "#8e7cc3"},
}

# 🔄 Преобразование HEX в RGB для Google Sheets API (0.0–1.0)
def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip("#")
    return {
        "red": round(int(hex_color[0:2], 16) / 255.0, 3),
        "green": round(int(hex_color[2:4], 16) / 255.0, 3),
        "blue": round(int(hex_color[4:6], 16) / 255.0, 3),
    }

# 🧠 Определение цвета по содержимому ячейки с кэшированием
def resolve_colors(text, color_cache):
    text = str(text).strip()
    if text in color_cache:
        return color_cache[text]

    # Стандартные цвета по умолчанию
    fg, bg = hex_to_rgb("#000000"), hex_to_rgb("#ffffff")

    if "x" in text:
        fg, bg = hex_to_rgb("#000000"), hex_to_rgb("#00ff00")
    elif "SH" in text and text != "TURKISH":
        fg, bg = hex_to_rgb("#ffffff"), hex_to_rgb("#000000")
    elif text in COLOR_MAP:
        colors = COLOR_MAP.get(text, {})
        fg = hex_to_rgb(colors.get("fg", "#000000"))
        bg = hex_to_rgb(colors.get("bg", "#ffffff"))

    color_cache[text] = (fg, bg)
    return fg, bg

# 🏗️ Генерация форматирующих repeatCell-запросов
def build_formatting_requests(values, sheet_id, start_row=0, start_col=3, log_file="logs/scanner_rotationsinfo.log"):
    # log_to_file(log_file, f"🖌️ Форматирование: {len(values)} строк × {len(values[0]) if values else 0} колонок")
    
    requests = []

    # 1️⃣ Общая заливка — D1:AC100
    default_fg = hex_to_rgb("#000000")
    default_bg = hex_to_rgb("#ffffff")

    total_rows = len(values)
    total_cols = len(values[0]) if values else 0

    # D = 3, AC = 29 (0-indexed, т.е. D1:AC100)
    # Теперь правая граница - AD (30)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": start_row + total_rows,
                "startColumnIndex": start_col,
                "endColumnIndex": start_col + total_cols,  # +1 для AD
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": default_bg,
                    "textFormat": {
                        "foregroundColor": default_fg
                    }
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
        }
    })

    # 1.5️⃣ Прямоугольники по меткам
    # Метки и их пары
    marker_pairs = [
        ("VIP", "Replacements VIP"),
        ("TURKISH", "Replacements TURKISH"),
        ("GENERIC", "Replacements GENERIC"),
        ("GSBJ", "Replacements GSBJ"),
        ("LEGENDZ", "Replacements LEGENDZ"),
        ("Tri-Star", "Replacements Tri-Star"),
        ("TritonRL", "Replacements TritonRL"),
        ("Shuffle", "Replacements Shuffle"),
    ]
    marker_colors = {
        "VIP": COLOR_MAP["VIP"],
        "TURKISH": COLOR_MAP["TURKISH"],
        "GENERIC": COLOR_MAP["GENERIC"],
        "GSBJ": COLOR_MAP["GSBJ"],
        "LEGENDZ": COLOR_MAP["LEGENDZ"],
        "Tri-Star": COLOR_MAP["TRISTAR"],
        "TritonRL": COLOR_MAP["TRITONRL"],
        "Shuffle": COLOR_MAP["Shuffle"],
    }
    # Поиск всех позиций меток
    marker_positions = {}
    for idx, row in enumerate(values):
        cell = str(row[0]).strip() if row else ""
        for marker, _ in marker_pairs:
            if cell == marker:
                marker_positions.setdefault(marker, []).append(idx)
        for _, repl in marker_pairs:
            if cell == repl:
                marker_positions.setdefault(repl, []).append(idx)
    # Для каждой пары строим прямоугольник
    for marker, repl in marker_pairs:
        if marker == "TritonRL":
            # Особая логика для TritonRL
            if marker in marker_positions:
                for m_idx in marker_positions[marker]:
                    # Сначала ищем Replacements TritonRL ниже
                    r_idx = None
                    if repl in marker_positions:
                        r_idx = next((i for i in marker_positions[repl] if i > m_idx), None)
                    if r_idx is None:
                        # Только если не нашли Replacements TritonRL, ищем ближайшую пустую ячейку в D ниже TritonRL
                        for i in range(m_idx + 1, 100):
                            if i >= len(values):
                                break
                            cell_val = str(values[i][0]).strip() if values[i] else ""
                            if cell_val == "":
                                r_idx = i
                                break
                        # Если не нашли пустую, или TritonRL слишком низко, то до конца
                        if r_idx is None or m_idx >= 97:
                            r_idx = 99  # 0-based, 100 строк
                    color = marker_colors.get(marker)
                    if not color:
                        continue
                    try:
                        fg = hex_to_rgb(color["fg"])
                        bg = hex_to_rgb(color["bg"])
                        # Верхняя грань (две линии)
                        for row_offset in [0, 1]:
                            requests.append({
                                "repeatCell": {
                                    "range": {
                                        "sheetId": sheet_id,
                                        "startRowIndex": start_row + m_idx + row_offset,
                                        "endRowIndex": start_row + m_idx + row_offset + 1,
                                        "startColumnIndex": start_col,
                                        "endColumnIndex": start_col + total_cols + 1,  # +1 для AD
                                    },
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": bg,
                                            "textFormat": {"foregroundColor": fg}
                                        }
                                    },
                                    "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                                }
                            })
                        # Нижняя грань (одна линия)
                        requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": start_row + r_idx,
                                    "endRowIndex": start_row + r_idx + 1,
                                    "startColumnIndex": start_col,
                                    "endColumnIndex": start_col + total_cols + 1,  # +1 для AD
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": bg,
                                        "textFormat": {"foregroundColor": fg}
                                    }
                                },
                                "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                            }
                        })
                        # Вертикальные грани (если есть пространство)
                        if r_idx - m_idx > 2:
                            for row in range(m_idx + 2, r_idx):
                                # Левая грань (D)
                                requests.append({
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": start_row + row,
                                            "endRowIndex": start_row + row + 1,
                                            "startColumnIndex": start_col,
                                            "endColumnIndex": start_col + 1,
                                        },
                                        "cell": {
                                            "userEnteredFormat": {
                                                "backgroundColor": bg,
                                                "textFormat": {"foregroundColor": fg}
                                            }
                                        },
                                        "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                                    }
                                })
                                # Правая грань (AD)
                                requests.append({
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": start_row + row,
                                            "endRowIndex": start_row + row + 1,
                                            "startColumnIndex": start_col + total_cols,  # AD
                                            "endColumnIndex": start_col + total_cols + 1,
                                        },
                                        "cell": {
                                            "userEnteredFormat": {
                                                "backgroundColor": bg,
                                                "textFormat": {"foregroundColor": fg}
                                            }
                                        },
                                        "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                                    }
                                })
                    except Exception:
                        continue
        else:
            if marker in marker_positions and repl in marker_positions:
                for m_idx in marker_positions[marker]:
                    r_idx = next((i for i in marker_positions[repl] if i > m_idx), None)
                    if r_idx is None:
                        continue
                    color = marker_colors.get(marker)
                    if not color:
                        continue
                    try:
                        fg = hex_to_rgb(color["fg"])
                        bg = hex_to_rgb(color["bg"])
                        # Верхняя грань (две линии)
                        for row_offset in [0, 1]:
                            requests.append({
                                "repeatCell": {
                                    "range": {
                                        "sheetId": sheet_id,
                                        "startRowIndex": start_row + m_idx + row_offset,
                                        "endRowIndex": start_row + m_idx + row_offset + 1,
                                        "startColumnIndex": start_col,
                                        "endColumnIndex": start_col + total_cols + 1,  # +1 для AD
                                    },
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": bg,
                                            "textFormat": {"foregroundColor": fg}
                                        }
                                    },
                                    "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                                }
                            })
                        # Нижняя грань (одна линия)
                        requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": start_row + r_idx,
                                    "endRowIndex": start_row + r_idx + 1,
                                    "startColumnIndex": start_col,
                                    "endColumnIndex": start_col + total_cols + 1,  # +1 для AD
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": bg,
                                        "textFormat": {"foregroundColor": fg}
                                    }
                                },
                                "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                            }
                        })
                        # Вертикальные грани (если есть пространство)
                        if r_idx - m_idx > 2:
                            for row in range(m_idx + 2, r_idx):
                                # Левая грань (D)
                                requests.append({
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": start_row + row,
                                            "endRowIndex": start_row + row + 1,
                                            "startColumnIndex": start_col,
                                            "endColumnIndex": start_col + 1,
                                        },
                                        "cell": {
                                            "userEnteredFormat": {
                                                "backgroundColor": bg,
                                                "textFormat": {"foregroundColor": fg}
                                            }
                                        },
                                        "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                                    }
                                })
                                # Правая грань (AD)
                                requests.append({
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": sheet_id,
                                            "startRowIndex": start_row + row,
                                            "endRowIndex": start_row + row + 1,
                                            "startColumnIndex": start_col + total_cols,  # AD
                                            "endColumnIndex": start_col + total_cols + 1,
                                        },
                                        "cell": {
                                            "userEnteredFormat": {
                                                "backgroundColor": bg,
                                                "textFormat": {"foregroundColor": fg}
                                            }
                                        },
                                        "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                                    }
                                })
                    except Exception:
                        continue
    color_cache = {}

    # 2️⃣ Заливка значений, начиная с E1 (то есть пропускаем первую колонку)
    for r_idx, row in enumerate(values):
        for c_idx, cell in enumerate(row[1:], start=1):  # Пропускаем первый столбец
            fg, bg = resolve_colors(cell, color_cache)

            if fg == default_fg and bg == default_bg:
                continue

            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row + r_idx,
                        "endRowIndex": start_row + r_idx + 1,
                        "startColumnIndex": start_col + c_idx,
                        "endColumnIndex": start_col + c_idx + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": bg,
                            "textFormat": {
                                "foregroundColor": fg
                            }
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)"
                }
            })

    # log_to_file(log_file, f"✅ Сформировано {len(requests)} форматирующих запросов.")
    return requests

# 🚀 Полный цикл применения форматирования по значениям
def format_sheet(
    service,
    spreadsheet_id,
    sheet_title,
    values,
    token_name,
    update_group,
    log_file,
    session,
    start_row=0,
    start_col=3,
    chunk_size=1500
):
    try:
        log_section(log_file, "format_sheet", f"🎨 Подготовка форматирования листа '{sheet_title}'")
        
        # Проверка входных данных
        if not values or not isinstance(values, list) or not all(isinstance(row, list) for row in values):
            log_error(log_file, "format_sheet", None, "invalid_data", "❌ Некорректные входные данные для форматирования.")
            return

        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = next(
            (s["properties"]["sheetId"] for s in sheet_metadata["sheets"]
             if s["properties"]["title"] == sheet_title),
            None
        )
        if sheet_id is None:
            raise ValueError(f"❌ Лист '{sheet_title}' не найден")

        try:
            formatting_requests = build_formatting_requests(values, sheet_id, start_row, start_col, log_file)
        except Exception as e:
            log_error(log_file, "format_sheet", None, "build_requests_fail", f"❌ Ошибка при создании запросов форматирования: {e}")
            return

        success = True

        for i in range(0, len(formatting_requests), chunk_size):
            chunk = formatting_requests[i:i + chunk_size]
            for attempt in range(3):
                try:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={"requests": chunk}
                    ).execute()
                    log_info(log_file, "format_sheet", None, "batch", f"✅ Отправлена порция форматирования {i}–{i + len(chunk)}.")
                    break
                except (socket.timeout, Exception) as e:
                    log_error(log_file, "format_sheet", None, "fail", f"❌ Попытка {attempt + 1} — ошибка в порции {i}–{i + len(chunk)}", exc=e)
                    if attempt < 2:
                        time.sleep(5)
                    else:
                        success = False

        if success:
            log_success(log_file, "format_sheet", None, "done", f"✅ Форматирование листа '{sheet_title}' завершено успешно.")
        else:
            log_warning(log_file, "format_sheet", None, "fail", f"⚠️ Форматирование листа '{sheet_title}' завершено с ошибками.")

    except Exception as e:
        log_error(log_file, "format_sheet", None, "fail", f"❌ Ошибка в format_sheet()", exc=e)
        raise
