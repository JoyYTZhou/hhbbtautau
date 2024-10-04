# HH $\to b\bar{b} \tau \tau$ Analysis Repo

This repository contains the code and configuration files for the HH $\to b\bar{b} \tau \tau$ analysis. The analysis is performed in the context of the [CMS experiment](https://cms.cern/). The project is coffea-based, and is tested mainly in LPC environment.

Current working branch: `submodule`.

Pull new changes in `src` directory by running `git submodule update --remote --merge` in the main directory. Or you could run `git submodule update --init --recursive` to update the submodule to the latest commit. See [here](https://git-scm.com/book/en/v2/Git-Tools-Submodules) for more details. Visit [CoffeaMate](https://github.com/JoyYTZhou/CoffeaMate) for more details on tools provided by `src` directory.

## Table of Contents
- [HH $\\to b\\bar{b} \\tau \\tau$ Analysis Repo](#hh-to-bbarb-tau-tau-analysis-repo)
  - [Table of Contents](#table-of-contents)
  - [Recommended Practice](#recommended-practice)
    - [Create copies of the repository for development and testing](#create-copies-of-the-repository-for-development-and-testing)
      - [Fork and Create a New Branch](#fork-and-create-a-new-branch)
      - [Using this Repo as a Template](#using-this-repo-as-a-template)
    - [Set up environment](#set-up-environment)
    - [Test event selection with a provided nanoaod file](#test-event-selection-with-a-provided-nanoaod-file)
    - [Obtain all MC samples needed from DAS](#obtain-all-mc-samples-needed-from-das)
    - [Submit batch jobs to LPC farm](#submit-batch-jobs-to-lpc-farm)
  - [Installation](#installation)
  - [Directory Structure](#directory-structure)

## Recommended Practice

### Create copies of the repository for development and testing

#### Fork and Create a New Branch
1. **Fork the Repository**:
   - Go to the repository page on GitHub.
   - Click the "Fork" button at the top right of the page to create a copy of the repository under your GitHub account.

2. **Clone the Forked Repository**:
   ```bash
   # Clone the forked repository
   git clone <your-forked-repository-url>

   # Navigate to the project directory
   cd <project-directory>
   ```

#### Using this Repo as a Template
Follow the instructions [here](https://help.github.com/en/github/creating-cloning-and-archiving-repositories/creating-a-repository-from-a-template) to create a new repository from this template.

### Set up environment
1. **Set up the environment in LPC**:
   - Run `source scripts/envsetup.sh` to set up a python virtual environment for the analysis. This script will install the necessary packages and set up the environment for running the analysis. It will also create a tarball of the environment for future use.
   - Run `source scripts/venv.sh` to set up a CMS-python environment with LCG software. This will activate the installed python virtual environment and set up the necessary environment variables for running the analysis. Do this everytime you want to work in this repo.
    - Run `source scripts/venv.sh --help` to see details on how to set up the environment.
2. **Modify the configuration files**:
   - Update the configuration files in the `configs` directory to reflect the correct paths to where the sample files are stored, where the output files should be saved, and other settings.
   - `projectconfig.py` contains all main configurations for the analysis, including the event selection setting, program runtime setting, the post-processing setting, and the plotting setting. In general, you could add as many new configurations (in the form of toml/yaml/json) as you want, and then load them in the `projectconfig.py` file so that you could use them in your python scripts.
   - check the `configs` directory for more details.

### Test event selection with a provided nanoaod file
1. Customize event selections through creating classes inherited from the `EventSelection` in the `analysis.evtselutil` module. Check `configs` directory for more details. Technically this should be the only directory you need to modify for cusmizing the event selection.
2. Test the correct working of the event selection classes by running
    ```bash
    source scripts/venv.sh
    python -m unittest tests.testproc
    ```
    - No changes to src code should be required, as they are not related to the event selection logic but only provides utility functions and object definitions. Any src code changes should be tested with the provided unit tests in the `tests` directory.
    - No changes to `main.py` should be required as it only provides the main program logic for running the analysis. 

### Obtain all MC samples needed from DAS
The current curling is heavily dependent on coffea pacakges and might be subject to change in the future. Navigate to `data` directory for more details.


### Submit batch jobs to LPC farm
The batch submission scripts provided in `exec` are compatible with HTCondor batch system. The python program will be executed with the tarball of the python virtual environment created in the `scripts/envsetup.sh` script. 

Other workflow management systems have been tried (e.g. local submission through lpcjobqueue, coffea singularity shells etc.) and are not recommended for now due to various conflicts.

1. **Generate json job files**:
  ```bash
  cd exec
  python genjobs.py
  ```
2. **Write jdl files**:
  A template of the job submission script `hhbbtt.sub` is provided in the `exec` directory.

## Installation
Instructions on how to install and set up the project.

```bash
git clone <repository-url-you-have-forked>
cd <project-directory>
git submodule update --init --recursive
```

## Directory Structure

A brief explanation of the different subdirectories in this repository:

- **src/**: Contains the source code for the analysis, including modules for event selection, object definitions, and utility functions. This is the [submodule](https://github.com/JoyYTZhou/CoffeaMate/tree/main)
  - **analysis/**: Modules related to the analysis logic and event selection.
  - **utils/**: Utility functions for file handling, data processing, and other common tasks.
  - **config/**: Configuration files for the analysis, including selection settings and parameter definitions.

- **configs/**: Configuration files for the analysis, including selection settings, environment settings (e.g. paths to data files), and parameter definitions for classes and functions.

- **tests/**: Contains unit tests and integration tests for the source code.
  - **test_filesysutil.py**: Tests for the file system utility functions.
  - **test_custom.py**: Tests for custom event selection classes.

- **data/**: Contains input data files and datasets used in the analysis.

- **results/**: Directory for storing the output results of the analysis, including plots, tables, and summary files.

- **scripts/**: Contains scripts for setting up the (LPC/LXPLUS) environment, running the analysis, and other automation tasks.

- **notebooks/**: Jupyter notebooks for exploratory data analysis, visualization, and prototyping.
