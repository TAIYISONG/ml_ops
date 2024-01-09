from inno_utils.loggers import log
import time

def timeit(func):
    """
    Decorator to execute a method and update its
    execution time as an attribute, called `execution_time`,
    of the instance class
    Parameters
    ----------
    func: function to time

    Returns
    -------
    returns time, also prints time in console
    """

    def timed(*args, **kwargs):
        msg_func_name = func.__name__

        msg = f"######## Start running task: '{msg_func_name}'."
        log.info(msg)

        ts = time.time()
        result = func(*args, **kwargs)
        te = time.time()

        msg_dur = time.strftime("%H:%M:%S", time.gmtime(te - ts))

        msg = f"######## Execution time of '{msg_func_name}': {msg_dur}."
        log.info(msg)

        return result

    return timed