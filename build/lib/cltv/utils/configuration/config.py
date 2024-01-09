from datetime import date
import enum
import os
import datetime as dt

from inno_utils.loggers import log

from cltv.utils.configuration.config_helpers import (
    get_config_files_in_dir,
    parse_config,
    generate_run_id,
)
import pipeline


class Singleton(object):
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass


class ConfigRunMode(str, enum.Enum):
    """
    This determines which blob storage will be used.
    """

    PROD = "prod"  # config_osa.yml + patch with prod_config_osa.yml
    DEV = "dev"  # config_osa.yml + patch with prod_config_osa.yml + patch with dev_config_osa.yml

    def __str__(self):
        return self.value


class ConfigRegion(str, enum.Enum):
    ONT = "ontario"
    WES = "west"
    QBC = "quebec"
    ATL = "atlantic"

    def __str__(self):
        return self.value


class Context:
    """
    A context object that is shared across all modules for extracting the configurations (*yml, run_id, run_mode).
    """

    def __init__(self) -> None:
        self.run_id = ""

    @property
    def region(self):
        return self._region

    @region.setter
    def region(self, value):
        if isinstance(value, ConfigRegion):
            self._region = value
        else:
            raise Exception(f"Not a valid region {value}")

    @property
    def run_id(self):
        return self._run_id

    @run_id.setter
    def run_id(self, value):
        if isinstance(value, str):
            self._run_id = value
        else:
            raise Exception("run_id must be a string type")

    @property
    def run_mode(self):
        return self._run_mode

    @run_mode.setter
    def run_mode(self, value):
        if isinstance(value, ConfigRunMode):
            self._run_mode = value
        else:
            raise Exception(f"Not a valid run mode {value}")

    @property
    def run_date(self):
        return self._run_date

    @run_date.setter
    def run_date(self, value):
        if isinstance(value, str):
            self._run_date = value
        else:
            raise Exception(f"Not a valid run date {value}")

    @property
    def prior_date(self):
        """The prior date is always the run date - 1 day"""
        prior_date = dt.datetime.strptime(self._run_date, "%Y-%m-%d") + dt.timedelta(
            days=-1
        )
        prior_date = dt.datetime.strftime(prior_date, "%Y-%m-%d")
        return prior_date

    @property
    def config(self):
        return self._config

    def set_session(
        self,
        region: str = None,
        run_id: str = None,
        run_mode: str = None,
        run_date: str = None,
        config_path: str = None,
    ):

        if not run_id:
            self.run_id = generate_run_id()

        # load config *yml files from default path or from users.
        if not config_path:
            config_path = os.path.join(pipeline.__path__[0], "configs")

        config_files = get_config_files_in_dir(config_path)
        self._config = parse_config(config_files)

        # Note: manual settings when calling set_session() COULD overwrite the settings in global.yml
        # 1. region
        if not region:
            region = ConfigRegion(self._config["region"])
            self.region = region
        else:
            self.region = ConfigRegion(region)

        # 2. run_mode
        if not run_mode:  # read run_mode from global.yml
            if self._config["run_mode"].lower() == "prod":
                self.run_mode = ConfigRunMode.PROD
                os.environ["NB_PROD"] = "True"  # so read_blob will read from prod env
            else:
                self.run_mode = ConfigRunMode.DEV
                os.environ["NB_PROD"] = ""

        else:  # read run_mode from user input when calling set_session()
            if run_mode == ConfigRunMode.PROD:
                self.run_mode = ConfigRunMode.PROD
                os.environ["NB_PROD"] = "True"  # so read_blob will read from prod env
            else:
                self.run_mode = ConfigRunMode.DEV
                os.environ["NB_PROD"] = ""
        # 3. run_date
        if not run_date:
            self.run_date = self._config["run_date"]
        else:
            self.run_date = run_date

        log.info(f"Using {self.run_mode} mode and {self.run_date} as current date")
