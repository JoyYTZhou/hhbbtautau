# Use settings from the config file

## Contents
- `projectconfig.py`: Loads the configurations from the `configs` directory and provides a way to access them in python scripts.
- `aodnamemap.yaml`: Contains the mapping between names of attributes in the AOD files and the names used in `selection.yaml`. This provides readability and flexibility when defining the event selection logic, e.g. switching between b-tagging algorithms.
- `selection.yaml`: Contains the event selection criteria.
- `customEvtSel.py`: Contains custom event selection classes coupled with selection.yaml that inherit from the `EventSelection` class in `analysis.evtselutil`. This is needed to further set up the event selection logic based on the threshold values. A `switch_selections` function is also provided to map the selection names to the corresponding classes so that runsetting.toml can be used to select the desired event selection.
- `runsetting.toml`: Contains the runtime settings for event selections, including whether the outputs are to be transferred into condor area, the name of the selection, and the job directory name.
- `dasksetting.toml`: CURRENTLY NOT USED. Contains the settings for the dask cluster, including the number of workers, the number of threads per worker, and the memory limit per worker.
- `postprocess.toml`: Contains the settings for the post-processing of the outputs, including the output directory to which the combined cutflow tables will be saved.
- `plotsetting.toml`: Contains dictionaries of how attributes are to be plotted, including the x-axis label, y-axis label, and the binning.