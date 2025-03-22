from dynaconf import Dynaconf
from pathlib import Path

root_path = Path(__file__).parent

mc_nm = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/aod_namemaps/MC_Namemap.yaml']
)

data_nm = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/aod_namemaps/Data_Namemap.yaml']
)

selection_sync = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/object_selections/sync.yaml'])

selection_vbf = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/object_selections/VBF.yaml']
)

selection_loose = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/object_selections/loosetau.yaml']
)

selection_tight = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/object_selections/tight.yaml'])

runsetting = Dynaconf(
    root_path = root_path,
    environments=True,
    settings_files=['config/runsetting.toml']
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
