# utils/clean.py

from database.session import SessionLocal
from database.db_models import RotationsInfo, SheetsInfo


def clear_db(table_name: str):
    model_map = {
        "SheetsInfo": SheetsInfo,
        "RotationsInfo": RotationsInfo,
        # Добавь сюда другие модели при необходимости
    }

    model = model_map.get(table_name)
    if not model:
        raise ValueError(f"❌ Неизвестная таблица: {table_name}")

    with SessionLocal() as session:
        session.query(model).update({
            model.last_scan: None,
            model.scan_quantity: 0,
            model.scan_failures: 0,
            model.last_update: None,
            model.update_quantity: 0,
            model.update_failures: 0,
            model.hash: None,
            model.values_json: None,
        })
        session.commit()
        print(f"✅ Таблица {table_name} успешно очищена.")

if __name__ == "__main__":
    # clear_db(table_name="RotationsInfo")
    clear_db(table_name="SheetsInfo")
