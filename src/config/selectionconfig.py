
from dynaconf import Dynaconf
import os as os

cwd = os.path.dirname(os.path.realpath(__file__))

settings = Dynaconf(
    root_path=cwd,
    environments=True,
    settings_files=['config/doubletau.yaml']
)

runsetting = Dynaconf(
    root_path=cwd,
    environments=True,
    settings_files=['config/runsetting.toml']
)
