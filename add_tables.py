import csv
from datetime import datetime
from sqlalchemy.orm import Session
from database.session import get_session  # Твой метод получения сессии
from database.db_models import GamingTable  # Импортируй свою модель

CSV_FILE = "gaming_tables.csv"  # Укажи путь к CSV

def parse_date(date_str):
    if not date_str or date_str.strip() == "":
        return None
    return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()

def parse_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def import_gaming_tables():
    with get_session() as session:  # type: Session
        with open(CSV_FILE, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)  # autodetect , as delimiter
            created, skipped = 0, 0

            for row in reader:
                local_name = row["local_name"].strip()
                table_id = row["table_id"].strip()
                active_from = parse_date(row["active_from"])

                # Проверка уникальности
                exists = session.query(GamingTable).filter_by(
                    local_name=local_name,
                    active_from=active_from,
                    table_id=table_id
                ).first()

                if exists:
                    print(f"⚠️ Запись уже существует: {local_name} - {active_from} - {table_id}")
                    skipped += 1
                    continue

                # status теперь строго int (1 или 0)
                status = parse_int(row.get("status"))
                if status is None:
                    status = 1  # Если в CSV пусто — ставим по умолчанию active=1

                table = GamingTable(
                    status=status,
                    local_name=local_name,
                    active_from=active_from,
                    active_until=parse_date(row.get("active_until")),
                    notes=row.get("notes", "").strip() or None,
                    table_id=table_id,
                    gaming_floor=row.get("gaming_floor", "").strip(),
                    dealers_game=row.get("dealers_game", "").strip(),
                    floor_number=parse_int(row.get("floor_number")),
                    end_user=row.get("end_user", "").strip() or None,
                    dui_nr=row.get("dui_nr", "").strip() or None,
                    vnc_ip=row.get("vnc_ip", "").strip() or None,
                    vnc_password=row.get("vnc_password", "").strip() or None,
                    rec_by=row.get("rec_by", "").strip() or None,
                    encoder_ip=row.get("encoder_ip", "").strip() or None,
                    encoder_password=row.get("encoder_password", "").strip() or None,
                )

                session.add(table)
                created += 1

            session.commit()
            print(f"✅ Импорт завершён: создано {created}, пропущено {skipped}")

if __name__ == "__main__":
    import_gaming_tables()
