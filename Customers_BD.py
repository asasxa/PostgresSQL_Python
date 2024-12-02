import psycopg2
from psycopg2 import IntegrityError


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(50) NOT NULL        
            );
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                phone VARCHAR(12) UNIQUE
            );
        """)
        print('База данных создана')


def is_valid_phone(phone):
    phone = phone.strip()
    if phone.startswith('+'):
        return len(phone) == 12 and phone[1:].isdigit()
    elif len(phone) == 11 and phone.isdigit():
        return True
    else:
        return False


def is_valid_email(email):
    return '@' in email and '.' in email.split('@')[-1]


def add_customers(conn, first_name, last_name, email, phones=None):
    if not is_valid_email(email):
        print(f"Ошибка: Некорректный формат email: {email}")
        return

    if phones:
        for phone in phones:
            if not is_valid_phone(phone):
                print(f"Ошибка: Некорректный формат номера {phone}")
                return

    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO customers (first_name, last_name, email) 
                           VALUES (%s, %s, %s) RETURNING id;""", (first_name, last_name, email))
            customer_id = cur.fetchone()[0]

            if phones:
                for phone in phones:
                    try:
                        cur.execute(
                            "INSERT INTO phones (customer_id, phone) VALUES (%s, %s);", (customer_id, phone))
                    except IntegrityError:
                        print(f"Телефон {phone} уже существует, клиент № {customer_id}")
            print(f"Клиент добавлен №: {customer_id}")

    except Exception as e:
        print(f"Ошибка добавления клиента: {e}")


def add_phone(conn, customer_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM phones WHERE customer_id = %s AND phone = %s;", (customer_id, phone))
            if cur.fetchone() is not None:
                print(f"Телефон {phone} уже существует, клиент № {customer_id}")
                return
            cur.execute("INSERT INTO phones (customer_id, phone) VALUES (%s, %s);", (customer_id, phone))
            print(f"Телефон {phone} добавлен для клиента № {customer_id}")
    except Exception as e:
        print(f"Ошибка при добавлении телефона: {e}")


def update_customer(conn, customer_id, new_first_name=None, new_last_name=None, new_email=None, new_phone=None):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE id = %s;", (customer_id,))
            customer = cur.fetchone()
            if not customer:
                print(f"Клиент {customer_id} не найден")
                return

            if new_first_name:
                cur.execute("UPDATE customers SET first_name = %s WHERE id = %s;", (new_first_name, customer_id))

            if new_last_name:
                cur.execute("UPDATE customers SET last_name = %s WHERE id = %s;", (new_last_name, customer_id))

            if new_email:
                cur.execute("UPDATE customers SET email = %s WHERE id = %s;", (new_email, customer_id))

            if new_phone is not None:
                add_phone(conn, customer_id, phone=new_phone)

            print(f"Данные о клиенте {customer_id} изменены.")
    except Exception as e:
        print(f"Ошибка изменения данных клиента: {e}")


def delete_phone(conn, customer_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM phones WHERE customer_id = %s AND phone = %s;", (customer_id, phone))
            if cur.fetchone() is None:
                print(f"Телефон {phone} не найден у клиента № {customer_id}")
                return
            cur.execute("DELETE FROM phones WHERE customer_id = %s AND phone = %s;", (customer_id, phone))
            print(f"Телефон {phone} удалён у клиента № {customer_id}")
    except Exception as e:
        print(f"Ошибка удаления телефона: {e}")


def delete_customer(conn, customer_id):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE id = %s;", (customer_id,))
            if cur.fetchone() is None:
                print(f"Клиент № {customer_id} не найден.")
                return

            cur.execute("DELETE FROM phones WHERE customer_id = %s;", (customer_id,))
            cur.execute("DELETE FROM customers WHERE id = %s;", (customer_id,))
            print(f"Клиент № {customer_id} удалён.")
    except Exception as e:
        print(f"Ошибка удаления клиента: {e}")


def find_customer(conn, first_name=None, last_name=None, email=None, phone=None):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT c.id, c.first_name, c.last_name, c.email, array_agg(p.phone) AS phones
                FROM customers c
                LEFT JOIN phones p ON p.customer_id = c.id
                WHERE TRUE
            """
            params = []

            if first_name:
                query += " AND c.first_name ILIKE %s"
                params.append("%" + first_name + "%")
            if last_name:
                query += " AND c.last_name ILIKE %s"
                params.append("%" + last_name + "%")
            if email:
                query += " AND c.email ILIKE %s"
                params.append("%" + email + "%")
            if phone:
                query += " AND p.phone ILIKE %s"
                params.append("%" + phone + "%")

            cur.execute(query, params)
            results = cur.fetchall()

            if results:
                for row in results:
                    phones = ', '.join(filter(None, row[4]))
                    print("ID: {}, Имя: {}, Фамилия: {}, Email: {}, Телефоны: {}".format(
                        row[0], row[1], row[2], row[3], phones))
            else:
                print("Клиент не найден.")
    except Exception as e:
        print(f"Ошибка поиска клиента: {e}")


def clear_database(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones CASCADE;")
        cur.execute("DROP TABLE IF EXISTS customers CASCADE;")
        print("База данных очищена.")


def show_table_data(conn, table_name):
    try:
        with conn.cursor() as cur:
            query = f"SELECT * FROM {table_name};"
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            print(f"\nДанные из таблицы {table_name}:")
            print("-" * 50)
            print(f"{' | '.join(columns)}")

            for row in results:
                print(f"{' | '.join(map(str, row))}")

            print("-" * 50)

    except Exception as e:
        print(f"Ошибка при получении данных из {table_name}: {e}")


if __name__ == "__main__":
    with psycopg2.connect(database='customersdb', user='postgres', password='89345672') as conn:
        create_db(conn)
        add_customers(conn, "Имя", "Фамилия", "имя_почтового_ящика@домен.ru", phones=["88005553535", "+78005553535"])
        find_customer(conn, first_name="Имя")
        update_customer(conn, 1, new_first_name="Новое Имя", new_last_name="Новая Фамилия", new_email="новое_имя_почтового_ящика@домен.ru", new_phone="88888888888")
        add_phone(conn, 1, "99999999999")
        find_customer(conn, first_name="Новое Имя")
        delete_phone(conn, 1, "88888888888")
        find_customer(conn, first_name="Новое Имя")
        delete_customer(conn, 1)
        find_customer(conn, first_name="Новое Имя")
        #show_table_data(conn, "customers")
        #show_table_data(conn, "phones")
        clear_database(conn)