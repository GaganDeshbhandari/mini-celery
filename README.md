# Mini-Celery

A lightweight distributed task queue inspired by Celery, built from scratch using Python, Redis, and PostgreSQL.

Mini-Celery provides asynchronous task execution, retries, dead letter queues, priority scheduling, worker heartbeats, dead worker recovery, PostgreSQL persistence, and a command-line interface for monitoring and management.

---

# Features

## Core Task Queue

* Asynchronous task execution
* Task registration using decorators
* Background worker processes
* Redis-based message broker

## Task Status Tracking

Supported task states:

```text
PENDING
RUNNING
SUCCESS
FAILED
REJECTED
```

Task status is stored in Redis and persisted to PostgreSQL.

---

## Retry System

* Automatic task retries
* Configurable retry limits
* Exponential backoff
* Retry scheduling via Redis Sorted Sets

Example:

```text
Attempt 1 → 5 seconds
Attempt 2 → 25 seconds
Attempt 3 → 125 seconds
```

---

## Dead Letter Queue (DLQ)

Tasks that exhaust all retries are moved to a Dead Letter Queue.

Benefits:

* Prevents infinite retry loops
* Preserves failed task payloads
* Allows later inspection and debugging

---

## Priority Queues

Four priority levels:

| Priority | Queue          |
| -------- | -------------- |
| 4        | critical_queue |
| 3        | high_queue     |
| 2        | medium_queue   |
| 1        | low_queue      |

Workers always process higher-priority tasks first.

---

## Worker Heartbeats

Workers periodically send heartbeats to Redis.

Benefits:

* Worker health monitoring
* Dead worker detection
* Worker activity tracking

---

## Dead Worker Recovery

If a worker crashes unexpectedly:

```text
Worker Crash
     ↓
Heartbeat Timeout
     ↓
Worker Marked DEAD
     ↓
Task Recovery
     ↓
Task Requeued
```

Processing tasks owned by dead workers are automatically recovered and requeued.

---

## PostgreSQL Persistence

Historical data is persisted to PostgreSQL.

### Tasks Table

Stores:

* Task metadata
* Status
* Priority
* Worker assignment
* Retry information

### Executions Table

Stores:

* Execution history
* Runtime duration
* Execution status

### Failures Table

Stores:

* Failure information
* Error messages
* Retry counts

### Workers Table

Stores:

* Worker metadata
* Last heartbeat
* Worker lifecycle status

---

## Worker Lifecycle Tracking

Workers can exist in three states:

```text
ONLINE
OFFLINE
DEAD
```

### ONLINE

Worker is actively sending heartbeats.

### OFFLINE

Worker terminated gracefully.

Example:

```bash
Ctrl + C
```

### DEAD

Worker crashed unexpectedly.

Example:

```bash
kill -9 <pid>
```

---

## Command Line Interface

Built-in monitoring and management commands.

### Worker Commands

```bash
mini-celery workers

mini-celery dead-workers

mini-celery worker-tasks <worker_id>
```

### Queue Commands

```bash
mini-celery queue-stats

mini-celery processing-queues

mini-celery retry-queue
```

### DLQ Commands

```bash
mini-celery dlq

mini-celery purge-dlq
```

### Task Commands

```bash
mini-celery task-status <task_id>

mini-celery task-info <task_id>
```

### Health Monitoring

```bash
mini-celery health
```

### Submit Tasks

```bash
mini-celery submit send_email \
    --args user@gmail.com \
           "Order Confirmed" \
           "Your order is on the way"
```

---

# Architecture

```text
                  Producer
                      │
                      ▼
                 Redis Broker
                      │
      ┌───────────────┼───────────────┐
      ▼               ▼               ▼

 Priority Queues   Retry Queue      DLQ

      ▼
   Worker
      │
      ▼

 Task Execution
      │
      ▼

 Redis Runtime State
      │
      ▼

 PostgreSQL Persistence

 ├── tasks
 ├── task_executions
 ├── task_failures
 └── workers
```

---

# Installation

## Clone Repository

```bash
git clone <repository-url>

cd mini-celery
```

---

## Create Virtual Environment

```bash
python -m venv venv

source venv/bin/activate
```

Windows:

```bash
venv\Scripts\activate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Redis Setup

Start Redis:

```bash
redis-server
```

Verify:

```bash
redis-cli ping
```

Expected:

```text
PONG
```

---

# PostgreSQL Setup

Create database:

```sql
CREATE DATABASE mini_celery;
```

Create required tables:

```text
tasks
workers
task_executions
task_failures
```

Configure database connection:

```python
taskqueue/database/db.py
```

---

# Creating Tasks

Example:

```python
from taskqueue.task import task


@task
def send_email(to, subject, body):
    print(
        f"Sending email to {to}"
    )
```

Submit:

```python
send_email.delay(
    "user@gmail.com",
    "Welcome",
    "Hello!"
)
```

---

# Running Components

## Start Worker

```bash
python -m taskqueue.worker
```

---

## Start Retry Scheduler

```bash
python -m taskqueue.retry_scheduler
```

---

## Start Recovery Scheduler

```bash
python -m taskqueue.recovery_scheduler
```

---

## Start Worker Monitor

```bash
python -m taskqueue.worker_monitor
```

---

# Example Workflow

```text
Task Submitted
      ↓
Redis Queue
      ↓
Worker Picks Task
      ↓
RUNNING
      ↓
SUCCESS
      ↓
Execution Logged
      ↓
Persisted To PostgreSQL
```

Failure Flow:

```text
Task Fails
      ↓
Retry Queue
      ↓
Retry Scheduler
      ↓
Task Re-executed
      ↓
Retries Exhausted
      ↓
Dead Letter Queue
      ↓
Failure Logged
```

---

# Technologies Used

* Python
* Redis
* PostgreSQL
* argparse
* threading
* JSON Serialization

---

# Future Improvements

* REST API
* Web Dashboard
* Metrics & Monitoring
* Prometheus Integration
* Multi-Node Workers
* Scheduled Tasks
* Task Chaining
* Result Backend

---

# Project Status

Current Version:

```text
Phase 10 Complete
```

Implemented:

* Task Queue
* Retries
* Dead Letter Queue
* Priority Scheduling
* Retry Scheduler
* Heartbeats
* Dead Worker Detection
* Dead Worker Recovery
* CLI
* PostgreSQL Persistence

Upcoming:

* Web Interface
* Monitoring Dashboard
* REST APIs
