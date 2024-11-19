import psycopg2
from psycopg2.sql import SQL, Identifier
from psycopg2.extras import RealDictCursor


# Функция для подключения к базе данных
def get_connection():
    return psycopg2.connect(
        dbname="clients_db",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )


# Функция для создания структуры базы данных
def create_tables():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS client_info (
                    client_id SERIAL PRIMARY KEY,
                    client_name VARCHAR(40) NOT NULL,
                    client_secondname VARCHAR(60) NOT NULL,
                    client_email VARCHAR(100) UNIQUE NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS client_phone (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER NOT NULL REFERENCES client_info(client_id) ON DELETE CASCADE,
                    phone VARCHAR(15)
                );
            """)


# Функция для добавления нового клиента
def add_client(client_name, client_secondname, client_email):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client_info (client_name, client_secondname, client_email)
                VALUES (%s, %s, %s)
                RETURNING client_id;
            """, (client_name, client_secondname, client_email))
            return cur.fetchone()[0]  # Возвращаем client_id


# Функция для добавления телефона клиенту
def add_phone(client_id, phone):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client_phone (client_id, phone)
                VALUES (%s, %s)
                RETURNING id, phone;
            """, (client_id, phone))
            return cur.fetchone()


# Функция для обновления данных клиента
def update_client(client_id, **kwargs):
    with get_connection() as conn:
        with conn.cursor() as cur:
            for field, value in kwargs.items():
                if value is not None:
                    cur.execute(
                        SQL("UPDATE client_info SET {} = %s WHERE client_id = %s")
                        .format(Identifier(field)),
                        (value, client_id)
                    )


# Функция для удаления телефона клиента
def delete_phone(client_id, phone):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM client_phone
                WHERE client_id = %s AND phone = %s
                RETURNING id;
            """, (client_id, phone))
            return cur.fetchone()


# Функция для удаления клиента
def delete_client(client_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM client_info
                WHERE client_id = %s
                RETURNING client_id;
            """, (client_id,))
            return cur.fetchone()


# Функция для поиска клиента
def find_client(**kwargs):
    conditions = []
    params = {}
    for field, value in kwargs.items():
        if value is not None:
            conditions.append(SQL("{} ILIKE %s").format(Identifier(field)))
            params[field] = f"%{value}%"

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = SQL("""
                SELECT c.client_id, c.client_name, c.client_secondname, c.client_email, p.phone
                FROM client_info c
                LEFT JOIN client_phone p ON c.client_id = p.client_id
            """)
            if conditions:
                query += SQL(" WHERE ") + SQL(" AND ").join(conditions)

            cur.execute(query, list(params.values()))
            return cur.fetchall()


if __name__ == "__main__":
    create_tables()

    client_id = add_client("Ivan", "Ivanov", "ivanov@example.com")
    print(f"Добавлен клиент с ID: {client_id}")

    phone_info = add_phone(client_id, "123456789")
    print(f"Добавлен телефон: {phone_info}")

    update_client(client_id, client_name="Ivan", client_secondname="Sidorov")
    print("Информация о клиенте обновлена.")

    results = find_client(client_name="Ivan")
    print("Результаты поиска:")
    for row in results:
        print(row)

    deleted_phone = delete_phone(client_id, "123456789")
    print(f"Удалён телефон: {deleted_phone}")

    deleted_client = delete_client(client_id)
    print(f"Удалён клиент с ID: {deleted_client}")
