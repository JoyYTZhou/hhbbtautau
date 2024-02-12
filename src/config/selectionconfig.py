from dynaconf import Dynaconf
from pathlib import Path

root_path = Path(__file__).parent

settings = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['src/config/doubletau.yaml']
)

runsetting = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['src/config/runsetting.toml']
)

dasksetting = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_file=['src/config/jobconfig.toml']
)
