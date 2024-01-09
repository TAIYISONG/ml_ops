#%%
import importlib
import os
import datetime as dt

from inno_utils.azure import write_blob, read_blob


from cltv.utils.configuration.config import ConfigRunMode, Context

# from cltv.utils.session import spark

context = Context()

# initialize with default settings in yml files.
context.set_session()


# test functionality
if __name__ == "__main__":
    context = Context()
    context.set_session()
    print(context.region)
    print(context.run_mode)
    print(context.run_date)
    print(context.prior_date)
    print(context.config["preprocess_data_outputs"]["core_feature_txn"])
    print(os.getenv("NB_PROD"))
# %%
