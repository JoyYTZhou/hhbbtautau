
from dynaconf import Dynaconf
import os as os

rootdir = os.environ['HHBBTT'] 

settings = Dynaconf(
    rootdir=rootdir,
    environments=True,
    settings_files=['src/config/doubletau.yaml']
)

runsetting = Dynaconf(
    rootdir=rootdir,
    environments=True,
    settings_files=['src/config/runsetting.toml']
)
