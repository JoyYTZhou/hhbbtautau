# Obtain and Preprocess Samples from DAS/random directory containing ROOT files.

## From DAS
1. **Prepare a list of sample names**:
  - Format the list of samples like `availableQuery.json` in the `data` directory.
2. **Run the following command**:
  - `python datacollect.py --query availableQuery.json`
  For more information, run `python datacollect.py --help`.
3. **Check the output**:
  - The output will be saved in the `data/preprocessed` directory.
  - The output will be a JSON.GZ file containing the list of preprocessed samples and their corresponding DAS paths.

## From custom directory