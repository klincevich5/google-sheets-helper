import psycopg2

conn = psycopg2.connect(
    dbname='scheduler',
    user='postgres',
    password='qweqwe',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

# Получаем имена всех столбцов, кроме id (учитываем регистр и схему)
cur.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'SheetsInfo'
      AND column_name != 'id'
    ORDER BY ordinal_position
""")
columns = [row[0] for row in cur.fetchall()]
columns_str = ', '.join([f'"{col}"' for col in columns])

# Вставляем копии строк (без id)
cur.execute(f'''
    INSERT INTO "SheetsInfo" ({columns_str})
    SELECT {columns_str} FROM "SheetsInfo"
''')

conn.commit()
cur.close()
conn.close()