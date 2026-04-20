"""TaskIQ tasks wrapping shared async implementations."""

from src.taskiq_app.app import broker
from src.tasks_common import cpu_bound, io_bound, mixed, noop

cpu_bound_task = broker.task(task_name="taskiq.cpu_bound")(cpu_bound)
io_bound_task = broker.task(task_name="taskiq.io_bound")(io_bound)
mixed_task = broker.task(task_name="taskiq.mixed")(mixed)
noop_task = broker.task(task_name="taskiq.noop")(noop)
