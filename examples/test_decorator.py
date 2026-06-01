import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from taskqueue.task import task, task_registry


@task
def send_email(to, subject, body):
    return f"Sent to {to}: {subject} {body}"


if __name__ == "__main__":
    print("send_email in registry:", "send_email" in task_registry)
    send_email.delay("test@example.com", "Hi", "Body")
    print(send_email("test@example.com", "Hi", "Body"))
