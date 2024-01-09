# Update the version if necessary
__version__ = 'v1.0' 

import datetime as dt
time_stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")

run_id = __version__ + "_" + time_stamp

try:
    ver_msg = "Version: " + eval("__version__")
    run_id_msg = "Run id: " + eval("run_id")
except:
    ver = ""
    run_id = time_stamp

msg = f"""
WELCOME TO CLTV!
{ver_msg}
{run_id_msg}
"""

print(msg)