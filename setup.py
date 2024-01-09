"""
Creates an whl file of the project
"""
# Uncomment the commented lines to update your virtual environment

from datetime import datetime

# import pkg_resources
from setuptools import find_packages, setup
import subprocess

NAME = "cltv"

def get_current_commit():
    """
    Returns current Git commit hash LOCALLY

    Returns
    -------
    str
        Commit hash
    """

    try:
        cm = "git rev-parse HEAD"
        output = subprocess.check_output(cm, shell=True)
        commit_hash = output.decode("UTF-8").strip()
    except Exception:
        print("Could not get local commit, defaulting to `None`")
        return None

    return commit_hash


with open("requirements.txt") as req:
    required = req.read().splitlines()

# exclude commented out lines
required = [x for x in required if not x.strip().startswith("#")]

try:
    code_version = "_" + get_current_commit()
except Exception:
    code_version = None

current_time = datetime.now().strftime("%Y%m%d.%H%M%S")

version = current_time + (code_version or "")

print(find_packages())

setup(
    name=NAME,
    version=version,
    package_data={"pipeline": ['configs/*.yml']},
    description="Customer Lifetime Value",
    author="Oliver",
    packages=find_packages(),
    install_requires=required,
    # python_requires=">=3.7.9", # comment this out as this version is bigger than 3.7.5 python version in databricks runtime 7.3 LTS
    entry_points={
        "console_scripts": ["cltv=cltv.cli.cltv_cli:main"],
    }
)
