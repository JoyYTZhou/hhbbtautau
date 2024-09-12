from dynaconf import Dynaconf
from pathlib import Path

root_path = Path(__file__).parent

namemap = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/aodnamemap.yaml']
)

selection = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/selection.yaml']
)

runsetting = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/runsetting.toml']
)

dasksetting = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_file=['config/dasksetting.toml']
)

cleansetting = Dynaconf(
    root_path=root_path,
    environments=True,
    settings_file=['config/postprocess.toml']
)

trainingsetting = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/trainingsetting.toml']
)
