import csv
from datetime import datetime
from sqlalchemy.orm import Session
from database.db_models import Person, EmployeePeriod
from database.session import get_session

CSV_FILE = "person_data.csv"

def parse_date(date_str):
    if not date_str or date_str.strip() == "":
        return None
    return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()

def populate_person_and_period():
    with get_session() as session:
        created_persons = 0
        created_periods = 0

        with open(CSV_FILE, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                full_name = row["GP Name"].strip()
                nickname = row.get("GP nickname", "").strip()
                start_date = parse_date(row.get("Date of creating"))
                end_date = parse_date(row.get("Date of destruction"))

                # Get or create Person (по full_name + dealer_nickname)
                person = session.query(Person).filter_by(
                    full_name=full_name,
                    dealer_nickname=nickname
                ).first()

                if not person:
                    person = Person(
                        full_name=full_name,
                        dealer_nickname=nickname
                    )
                    session.add(person)
                    session.flush()  # Чтобы получить person.id
                    created_persons += 1

                # Create EmployeePeriod if not exists
                period_exists = session.query(EmployeePeriod).filter_by(
                    person_id=person.id,
                    date_start=start_date,
                    date_end=end_date
                ).first()

                if not period_exists:
                    period = EmployeePeriod(
                        person_id=person.id,
                        date_start=start_date,
                        date_end=end_date
                    )
                    session.add(period)
                    created_periods += 1

        session.commit()
        print(f"✅ Добавлено персон: {created_persons}")
        print(f"✅ Добавлено периодов: {created_periods}")
        print("\n📋 Текущий список персон и периодов:")

        # Печать всех персон и их периодов
        for person in session.query(Person).order_by(Person.full_name).all():
            print(f"👤 {person.full_name} ({person.dealer_nickname})")
            for period in person.employment_periods:
                print(f"   └ 📆 {period.date_start} – {period.date_end or 'настоящее время'}")

if __name__ == "__main__":
    populate_person_and_period()
