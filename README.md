# HH $\to b\bar{b} \tau \tau$ Analysis Repo

This repository contains the code and configuration files for the HH $\to b\bar{b} \tau \tau$ analysis. The analysis is performed in the context of the [CMS experiment](https://cms.cern/).

## Table of Contents
- [HH $\\to b\\bar{b} \\tau \\tau$ Analysis Repo](#hh-to-bbarb-tau-tau-analysis-repo)
  - [Table of Contents](#table-of-contents)
  - [Using this Repo as a Template](#using-this-repo-as-a-template)
  - [Installation](#installation)
  - [Directory Structure](#directory-structure)

## Using this Repo as a Template
Follow the instructions [here](https://help.github.com/en/github/creating-cloning-and-archiving-repositories/creating-a-repository-from-a-template) to create a new repository from this template.


## Installation

Instructions on how to install and set up the project.

```bash
# Clone the repository
git clone <repository-url>

# Navigate to the project directory
cd <project-directory>

# Initialize and update submodules
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
