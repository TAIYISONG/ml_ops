from cltv.src.etl.preproc_trx.main import task_process_trx

task_process_trx()
from cltv.utils.configuration import context

context.set_session(run_date="2021-10-29")

task_process_trx()
