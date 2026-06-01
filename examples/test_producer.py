import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task


@task
def send_email(to, subject, body):
    return f"Sent to {to}: {subject}"


if __name__ == "__main__":
    task_id = send_email.delay("gagan@gmail.com", "Order Confirmed", "Biryani coming")
    print("enqueued task id:", task_id)
