# utils/floor_resolver

def get_floor_by_table_name(table_name: str, floor_map: dict) -> str:
    for floor, tables in floor_map.items():
        if table_name in tables:
            return floor
    return "UNKNOWN"