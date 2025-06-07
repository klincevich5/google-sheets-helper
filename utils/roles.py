ROLE_LEVELS = {
    "dealer": 1,
    "driver": 1,
    "qa": 2,
    "hr": 2,
    "manager": 2,
    "super_admin": 3,
}

ROLE_PERMISSIONS = {
    "dealer": {
        "feedback_self": True,
        "feedback_team": False,
        "view_tasks": False,
        "view_dealers": True,
        "view_reports": False,
    },
    "manager": {
        "feedback_self": True,
        "feedback_team": True,
        "view_tasks": True,
        "view_dealers": True,
        "view_reports": True,
    },
    "super_admin": {
        "feedback_self": True,
        "feedback_team": True,
        "view_tasks": True,
        "view_dealers": True,
        "view_reports": True,
    },
}
