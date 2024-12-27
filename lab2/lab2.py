import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


class Postgres():
    def __init__(self, creds):
        self.creds = creds
        self.conn, self.cursor = self.new_connection()

        self.create_table()


    def new_connection(self):
        conn = psycopg2.connect(**self.creds)
        cursor = conn.cursor()
        return conn, cursor


    def print(self, *args, **largs):
        print(f"{datetime.now().strftime('%H:%M:%S')}  ", *args, **largs)


    def create_table(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_counter (
                user_id SERIAL PRIMARY KEY,
                counter INTEGER NOT NULL DEFAULT 0,
                version INTEGER NOT NULL DEFAULT 0
            )
        """
        )

        self.cursor.execute("SELECT COUNT(*) FROM user_counter WHERE user_id = 1")
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute("INSERT INTO user_counter (counter, version) VALUES (0, 0)")

        self.conn.commit()


    def set_zero(self):
        self.cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1")
        self.conn.commit()


    def update(self, query_func):
        conn, cursor = self.new_connection()

        for i in range(10_000):
            query_func(cursor, i)
            conn.commit()

        cursor.close()
        conn.close()


    def lost_update(self, cursor, iteration):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))


    def inplace_update(self, cursor, iteration):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1 RETURNING counter")


    def row_level_locking(self, cursor, iteration):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter,))


    def optimistic_concurrency(self, cursor, iteration):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            counter, version = cursor.fetchone()
            counter += 1
            cursor.execute(
                "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = 1 AND version = %s",
                (counter, version + 1, version),
            )
            if cursor.rowcount > 0:
                break


    def run_threads(self, query_func, label):
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.update, query_func) for _ in range(10)]
            for future in futures:
                future.result()


    def main(self):
        for task in [[self.lost_update, "Lost-update"], [self.inplace_update, "In-place Update"], [self.row_level_locking, "Row-level Locking"], [self.optimistic_concurrency, "Optimistic Concurrency Control"]]:
            start_time = time.time()
            self.run_threads(*task)
            self.print(f"{task[1]} done in {int(time.time() - start_time)}s")
            self.set_zero()



if __name__ == "__main__":
    creds = {
        "host": "127.0.0.1",
        "port": "5432",
        "user": "postgres",
        "password": "strongpassword",
        "database": "pvs"
    }


    pg = Postgres(creds)
    pg.main()

