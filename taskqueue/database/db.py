import psycopg2


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="mini_celery",
        user="gagan",
        password="Gagan64014"
    )


# from taskqueue.database.db import get_connection

conn = get_connection()


conn.close()