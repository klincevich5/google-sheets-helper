FLOORS={"VIP": ["vBJ2", "vBJ3", "gBC1", "vBC3", "vBC4", "vHSB1", "vDT1", "gsRL1", "swBC1", "swRL1"],
        "TURKISH": ["tBJ1", "tBJ2", "tRL1"],
        "GENERIC": ["gBJ1", "gBJ3", "gBJ4", "gBJ5", "gBC2", "gBC3", "gBC4", "gBC5", "gBC6", "gRL1", "gRL2", "gARL1"],
        "GSBJ": ["gsBJ1", "gsBJ2", "gsBJ3", "gsBJ4", "gsBJ5", "gRL3"],
        "LEGENDZ": ["lBJ1", "lBJ2", "lBJ3"],
        "TRISTAR": ["DT", "DTL", "TP", "AB", "L7"],
        "TRITON": ["TritonRL"]
        }

GAME_PERMITS_TO_TABLE={"BJ": ["vBJ2", "vBJ3", "gBJ1", "gBJ3", "gBJ4", "gBJ5", "gsBJ1", "gsBJ2", "gsBJ3", "gsBJ4", "gsBJ5", "tBJ1", "tBJ2"],
                       "BC": ["vBC3", "vBC4", "gBC1", "gBC2", "gBC3", "gBC4", "gBC5", "gBC6"],
                       "RL": ["gsRL1", "gRL1", "gRL2", "gRL3", "tRL1"],
                       "DT": ["vDT1"],
                       "HSB": ["vHSB1"],
                       "swBJ": ["lBJ1", "lBJ2", "lBJ3"],
                       "swBC": ["swBC1"],
                       "swRL": ["swRL1"],
                       "SH": ["SH", "SH/VIP", "SH/GEN", "SH/GSBJ", "SH/LZ"],
                       "TritonRL": ["TritonRL"]
                          }

{
  "date": "2025-05-11",
  "dealer": {
    "full_name": "Ivan Ivanov",
    "nickname": ["Oliver", "Oli"]
  },
  "permits": {
    "floors": {
      "VIP": False,
      "GENERIC": False,
      "GSBJ": False,
      "TURKISH": False,
      "LEGENDZ": False,
      "TRISTAR": False,
      "TRITON": False
    },
    "games": {
      "Male": False,
      "BJ": False,
      "BC": False,
      "RL": False,
      "DT": False,
      "HSB": False,
      "swBJ": False,
      "swBC": False,
      "swRL": False,
      "SH": False,
      "TritonRL": False
    }
  },
  "shifts": [
    {
      "type": "Day",
      "is_scheduled": True,
      "is_additional": False,
      "is_extra": False,
      "is_sickleave": False,
      "is_vacation": False,
      "is_did_not_come": False,
      "is_left_the_shift": False,
      "start": "09:00",
      "end": "21:00",

      "assigned_floors": ["VIP", "LEGENDZ"],

      "rotation": [
        {
          "floor": "VIP",
          "schedule": [
            {"time": "09:00", "table": ""},
            {"time": "09:30", "table": ""},
            ...
          ]
        },
        {
          "floor": "LEGENDZ",
          "schedule": [
            {"time": "10:00", "table": ""},
            {"time": "10:30", "table": ""},
            ...
          ]
        }
      ],

      "mistakes": [
        {
          "table": "vBJ2",
          "entries": [
            {"time": "09:45", "issue": "Failed to scan a card"}
          ]
        },
        {
          "table": "gsRL1",
          "entries": [
            {"time": "10:30", "issue": "No scan"},
            {"time": "11:00", "issue": "No scan"},
            ...
          ]
        }
      ],

      "feedbacks": [
        {
          "id": "69",
          "topic": "Good Card Placement",
          "floor": "VIP",
          "game_type": "BJ",
          "reported_by": "SM Anton",
          "comment": "Very precise dealing",
          "is_proof": False,
          "action_taken": "Rebriefed",
          "forwarded_by": "QA Supervisor",
          "after_forward_comment": "Will monitor next shift"
        },
        {
          "id": "209",
          "topic": "Bad Card Placement",
          "floor": "VIP",
          "game_type": "GENERIC",
          "reported_by": "SM Anton",
          "comment": "Cards were not placed correctly",
          "action_taken": "Rebriefed",
          "forwarded_by": "QA Supervisor",
          "after_forward_comment": "Will monitor next shift"
        }
      ]
    }
  ]
}

# Пример итоговой структуры для MonitoringStorage (плоская форма):
# Это то, что сохраняется в БД и используется в MonitoringStorageTask
MONITORING_STORAGE_EXAMPLE = {
    "dealer_name": "Ivan Ivanov",
    "dealer_nicknames": ["Oliver", "Oli"],
    "report_date": "2025-05-11",
    "shift_type": "Day",
    "shift_start": "09:00",
    "shift_end": "21:00",
    "break_number": 0,
    "is_scheduled": True,
    "is_additional": False,
    "is_extra": False,
    "is_sickleave": False,
    "is_vacation": False,
    "is_did_not_come": False,
    "is_left_the_shift": False,
    "assigned_floors": ["VIP", "LEGENDZ"],
    "floor_permits": {
        "VIP": False, "GENERIC": False, "GSBJ": False, "TURKISH": False, "LEGENDZ": False, "TRISTAR": False, "TRITON": False
    },
    "game_permits": {
        "Male": False, "BJ": False, "BC": False, "RL": False, "DT": False, "HSB": False, "swBJ": False, "swBC": False, "swRL": False, "SH": False, "TritonRL": False
    },
    "has_mistakes": True,
    "has_feedbacks": True,
    "rotation": [
        {
            "floor": "VIP",
            "schedule": [
                {"time": "09:00", "table": ""},
                {"time": "09:30", "table": ""}
            ]
        },
        {
            "floor": "LEGENDZ",
            "schedule": [
                {"time": "10:00", "table": ""},
                {"time": "10:30", "table": ""}
            ]
        }
    ],
    "mistakes": [
        {"table": "vBJ2", "entries": [{"time": "09:45", "issue": "Failed to scan a card"}]},
        {"table": "gsRL1", "entries": [{"time": "10:30", "issue": "No scan"}, {"time": "11:00", "issue": "No scan"}]}
    ],
    "feedbacks": [
        {"id": "69", "topic": "Good Card Placement", "floor": "VIP", "game_type": "BJ", "reported_by": "SM Anton", "comment": "Very precise dealing", "is_proof": False, "action_taken": "Rebriefed", "forwarded_by": "QA Supervisor", "after_forward_comment": "Will monitor next shift"},
        {"id": "209", "topic": "Bad Card Placement", "floor": "VIP", "game_type": "GENERIC", "reported_by": "SM Anton", "comment": "Cards were not placed correctly", "action_taken": "Rebriefed", "forwarded_by": "QA Supervisor", "after_forward_comment": "Will monitor next shift"}
    ],
    "raw_data": {}  # исходный json, если нужно
}
