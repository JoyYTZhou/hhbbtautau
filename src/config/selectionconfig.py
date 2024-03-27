from dynaconf import Dynaconf
from pathlib import Path

root_path = Path(__file__).parent

selectionsettings = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/bbtthadronic.yaml']
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
    settings_file=['config/dataclean.toml']
)

plotsetting = Dynaconf(
    root_path=root_path,
    environments=True,
    settings_file=['config/plotsetting.yaml']
)
