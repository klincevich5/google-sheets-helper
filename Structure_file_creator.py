import os

IGNORED = {"__pycache__", "venv", ".git"}

def walk(path, prefix=""):
    entries = sorted(os.listdir(path))
    for i, entry in enumerate(entries):
        if entry in IGNORED or entry.endswith(".pyc") or entry.endswith(".session-journal"):
            continue
        full_path = os.path.join(path, entry)
        is_last = i == len(entries) - 1
        branch = "└── " if is_last else "├── "
        print(prefix + branch + entry)
        if os.path.isdir(full_path):
            extension = "    " if is_last else "│   "
            walk(full_path, prefix + extension)

with open("STRUCTURE.md", "w", encoding="utf-8") as f:
    from contextlib import redirect_stdout
    with redirect_stdout(f):
        print("Project structure:")
        walk(".")
