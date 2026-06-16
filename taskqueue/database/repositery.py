from datetime import datetime, timedelta

from taskqueue.database.db import get_connection


def create_task(
    task_id,
    task_name,
    priority,
    retries=0
):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO tasks (
            task_id,
            task_name,
            status,
            priority,
            retries,
            created_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """,
        (
            task_id,
            task_name,
            "PENDING",
            priority,
            retries,
            datetime.now()
        )
    )

    conn.commit()

    cur.close()
    conn.close()


def update_task_status(
    task_id,
    status,
    worker_id=None
):
    conn = get_connection()
    cur = conn.cursor()

    if status == "RUNNING":

        cur.execute(
            """
            UPDATE tasks
            SET status=%s,
                worker_id=%s,
                started_at=%s
            WHERE task_id=%s
            """,
            (
                status,
                worker_id,
                datetime.now(),
                task_id
            )
        )

    elif status in ("SUCCESS", "FAILED"):

        cur.execute(
            """
            UPDATE tasks
            SET status=%s,
                completed_at=%s
            WHERE task_id=%s
            """,
            (
                status,
                datetime.now(),
                task_id
            )
        )

    else:

        cur.execute(
            """
            UPDATE tasks
            SET status=%s
            WHERE task_id=%s
            """,
            (
                status,
                task_id
            )
        )

    conn.commit()

    cur.close()
    conn.close()


def log_execution(
    task_id,
    worker_id,
    started_at,
    completed_at,
    status
):
    conn = get_connection()
    cur = conn.cursor()

    duration = (
        completed_at - started_at
    ).total_seconds()

    cur.execute(
        """
        INSERT INTO task_executions (
            task_id,
            worker_id,
            started_at,
            completed_at,
            duration,
            status
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """,
        (
            task_id,
            worker_id,
            started_at,
            completed_at,
            duration,
            status
        )
    )

    conn.commit()

    cur.close()
    conn.close()

def log_failure(
    task_id,
    error_message,
    retry_count
):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO task_failures (
            task_id,
            error_message,
            failed_at,
            retry_count
        )
        VALUES (
            %s,
            %s,
            %s,
            %s
        )
        """,
        (
            task_id,
            error_message,
            datetime.now(),
            retry_count
        )
    )

    conn.commit()

    cur.close()
    conn.close()


def upsert_worker(
    worker_id,
    status
):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO workers (
            worker_id,
            status,
            registered_at,
            last_seen
        )
        VALUES (
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (worker_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            last_seen = EXCLUDED.last_seen
        """,
        (
            worker_id,
            status,
            datetime.now(),
            datetime.now()
        )
    )

    conn.commit()

    cur.close()
    conn.close()