# Use settings from the config file

## Contents
- `projectconfig.py`: Loads the configurations from the `configs` directory and provides a way to access them in python scripts.
- `aodnamemap.yaml`: Contains the mapping between names of attributes in the AOD files and the names used in `selection.yaml`. This provides readability and flexibility when defining the event selection logic, e.g. switching between b-tagging algorithms.
- `selection.yaml`: Contains the event selection logic for the analysis.
- `customEvtSel.py`: Contains custom event selection classes that inherit from the `EventSelection` class in `analysis.evtselutil`. This is needed to further set up the event selection logic.
- `runsetting.toml`: Contains the runtime settings for the analysis, including whether the outputs are to be transferred into condor area, the name of the selection, and the job directory name.