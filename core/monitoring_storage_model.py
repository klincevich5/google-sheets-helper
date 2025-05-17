# core/monitoring_storage_model.py

from datetime import datetime, date, time

class MonitoringStorageTask:
    def __init__(self, data):
        self.id = data.get("id")
        self.dealer_name = data.get("dealer_name")
        self.dealer_nicknames = data.get("dealer_nicknames", [])
        self.report_date = self._parse_date(data.get("report_date"))

        self.shift_type = data.get("shift_type")
        self.shift_start = self._parse_time(data.get("shift_start"))
        self.shift_end = self._parse_time(data.get("shift_end"))

        self.break_number = data.get("break_number", 0)

        self.is_scheduled = data.get("is_scheduled", False)
        self.is_additional = data.get("is_additional", False)
        self.is_extra = data.get("is_extra", False)
        self.is_sickleave = data.get("is_sickleave", False)
        self.is_vacation = data.get("is_vacation", False)
        self.is_did_not_come = data.get("is_did_not_come", False)
        self.is_left_the_shift = data.get("is_left_the_shift", False)

        self.assigned_floors = data.get("assigned_floors", [])
        self.floor_permits = data.get("floor_permits", {})
        self.game_permits = data.get("game_permits", {})

        self.has_mistakes = data.get("has_mistakes", False)
        self.has_feedbacks = data.get("has_feedbacks", False)

        self.rotation = data.get("rotation", [])
        self.mistakes = data.get("mistakes", [])
        self.feedbacks = data.get("feedbacks", [])
        self.raw_data = data.get("raw_data", {})

    def _parse_date(self, value):
        if not value:
            return None
        if isinstance(value, date):
            return value
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except Exception:
            return None

    def _parse_time(self, value):
        if not value:
            return None
        if isinstance(value, time):
            return value
        try:
            return datetime.strptime(value, "%H:%M:%S").time()
        except Exception:
            try:
                return datetime.strptime(value, "%H:%M").time()
            except Exception:
                return None

    def to_dict(self):
        return {
            "id": self.id,
            "dealer_name": self.dealer_name,
            "dealer_nicknames": self.dealer_nicknames,
            "report_date": self.report_date.isoformat() if self.report_date else None,
            "shift_type": self.shift_type,
            "shift_start": self.shift_start.isoformat() if self.shift_start else None,
            "shift_end": self.shift_end.isoformat() if self.shift_end else None,
            "break_number": self.break_number,
            "is_scheduled": self.is_scheduled,
            "is_additional": self.is_additional,
            "is_extra": self.is_extra,
            "is_sickleave": self.is_sickleave,
            "is_vacation": self.is_vacation,
            "is_did_not_come": self.is_did_not_come,
            "is_left_the_shift": self.is_left_the_shift,
            "assigned_floors": self.assigned_floors,
            "floor_permits": self.floor_permits,
            "game_permits": self.game_permits,
            "has_mistakes": self.has_mistakes,
            "has_feedbacks": self.has_feedbacks,
            "rotation": self.rotation,
            "mistakes": self.mistakes,
            "feedbacks": self.feedbacks,
            "raw_data": self.raw_data,
        }
