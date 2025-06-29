import csv
from datetime import datetime
from database.db_models import Person
from database.session import get_session  # или твой способ подключения

CSV_FILE = "person_data.csv"  # путь к CSV

def parse_date(date_str):
    if not date_str or date_str.strip() == "":
        return None
    return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()

def populate_persons_from_csv():
    with get_session() as session:
        created, updated, skipped = 0, 0, 0

        with open(CSV_FILE, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                full_name = row["GP Name"].strip()
                nickname = row.get("GP nickname", "").strip()
                start_date = parse_date(row.get("Date of creating"))
                end_date = parse_date(row.get("Date of destruction"))

                if not full_name or not nickname:
                    print(f"⚠️ Пропущена строка без имени или никнейма: {row}")
                    skipped += 1
                    continue

                person = session.query(Person).filter_by(
                    full_name=full_name,
                    dealer_nickname=nickname
                ).first()

                if not person:
                    person = Person(
                        full_name=full_name,
                        dealer_nickname=nickname,
                        date_start=start_date,
                        date_end=end_date
                    )
                    session.add(person)
                    created += 1
                else:
                    # обновление
                    person.date_start = start_date
                    person.date_end = end_date
                    updated += 1

        session.commit()
        print(f"✅ Создано персон: {created}")
        print(f"✅ Обновлено персон: {updated}")
        print(f"⚠️ Пропущено строк: {skipped}")

        # Вывод всех персон
        print("\n📋 Список персон:")
        for person in session.query(Person).order_by(Person.full_name).all():
            print(f"👤 {person.full_name} ({person.dealer_nickname})")
            print(f"   📆 {person.date_start} – {person.date_end or 'настоящее время'}")

if __name__ == "__main__":
    print("🔄 Начинаем заполнение персон из CSV...")
    populate_persons_from_csv()
    print("✅ Заполнение завершено!")