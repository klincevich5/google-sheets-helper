from datetime import date
from dateutil.relativedelta import relativedelta
from database.session import get_session
from database.db_models import Person, DealerMonthlyStatus


def month_range(start_date, end_date):
    """Генератор первого числа каждого месяца от start_date до end_date."""
    current = date(start_date.year, start_date.month, 1)
    while current <= end_date:
        yield current
        current += relativedelta(months=1)


def create_dealer_statuses():
    """Создаёт DealerMonthlyStatus для всех месяцев каждого периода работы."""
    created = 0
    with get_session() as session:
        persons = session.query(Person).all()

        for person in persons:
            all_periods = person.employment_periods

            for period in all_periods:
                if not period.date_start:
                    continue

                date_start = period.date_start
                date_end = period.date_end or date.today()

                print(f"🔄 {person.full_name}: период {date_start} → {date_end}")

                for month_start in month_range(date_start, date_end):
                    exists = session.query(DealerMonthlyStatus).filter_by(
                        person_id=person.id,
                        related_month=month_start
                    ).first()

                    if not exists:
                        # Собрать никнеймы за этот месяц
                        nicknames = set()
                        for p in all_periods:
                            if not p.date_start:
                                continue
                            period_start = p.date_start
                            period_end = p.date_end or date.today()

                            if period_start <= month_start <= period_end:
                                if person.dealer_nickname:
                                    nicknames.add(person.dealer_nickname)

                        dms = DealerMonthlyStatus(
                            person_id=person.id,
                            related_month=month_start,
                            dealer_nicknames=list(nicknames),
                            schedule=False,
                            bonus=False,
                            qa_list=False,
                            feedback_status=False
                        )
                        session.add(dms)
                        created += 1

        session.commit()
        print(f"\n✅ Записей создано: {created}")


if __name__ == "__main__":
    print("🔄 Начинаем добавление статусов для всех персон...")
    create_dealer_statuses()
    print("✅ Все статусы успешно добавлены!")
