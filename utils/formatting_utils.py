# utils/formatting_utils.py

from utils.logger import log_to_file

def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip("#")
    return {
        "red": int(hex_color[0:2], 16) / 255,
        "green": int(hex_color[2:4], 16) / 255,
        "blue": int(hex_color[4:6], 16) / 255,
    }

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
}

def build_formatting_requests(values, sheet_id, start_row=0, start_col=3):
    log_to_file("logs/scanner_rotationsinfo.log", f"🖌️ Форматирую {len(values)} строк и {len(values[0])} колонок")
    for row in values:
        log_to_file("logs/scanner_rotationsinfo.log", f"🖌️ {row}")
    requests = []
    for r_idx, row in enumerate(values):
        for c_idx, cell in enumerate(row):
            text = str(cell).strip()
            matched = False

            if "x->" in text:
                fg = hex_to_rgb("#000000")
                bg = hex_to_rgb("#00ff00")
                matched = True
            elif "SH" in text:
                fg = hex_to_rgb("#ffffff")
                bg = hex_to_rgb("#000000")
                matched = True
            elif text in COLOR_MAP:
                colors = COLOR_MAP[text]
                fg = hex_to_rgb(colors["fg"])
                bg = hex_to_rgb(colors["bg"])
                matched = True
            else:
                fg = hex_to_rgb("#000000")
                bg = hex_to_rgb("#ffffff")

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
    return requests