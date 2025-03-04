import subprocess
from tabulate import tabulate

def query_datasets():
    """
    Prompts the user for dataset search parameters, queries DAS using dasgoclient,
    and returns matching datasets.

    Returns:
    - list: A list of matching dataset names.
    """
    
    # Prompt user for input
    prefix = input("Enter dataset name prefix (e.g., TTToSemiLeptonic): ").strip()
    year = input("Enter year (e.g., 2022PostEE): ").strip()
    nanoaod_version = input("Enter NANOAOD version (or press enter to skip): ").strip()
    data_tier = input("Enter data tier (NANOAOD or MINIAOD): ").strip().upper()

    # Validate input
    if not prefix or not year:
        print("Prefix and year are required fields.")
        return []

    if data_tier not in ["NANOAOD", "MINIAOD"]:
        print("Invalid data tier! Please enter 'NANOAOD' or 'MINIAOD'.")
        return []

    # Construct DAS query
    query = f"dataset dataset={prefix}*/*{year}*/{data_tier}*"
    if nanoaod_version:
        query = f"dataset dataset={prefix}*/*{year}*v{nanoaod_version}*/{data_tier}*"

    print("\nRunning DAS query:", query)

    try:
        result = subprocess.run(
            ["dasgoclient", "-query", query],
            capture_output=True,
            text=True,
            check=True
        )
        datasets = result.stdout.strip().split("\n")
        datasets = [ds for ds in datasets if ds]  # Remove empty lines

        if not datasets:
            print("\nNo matching datasets found.")
            return None

        # Print results in a table format
        table_data = [[i, ds] for i, ds in enumerate(datasets, 1)]
        print("\nMatching Datasets:\n")
        print(tabulate(table_data, headers=["Index", "Dataset Name"], tablefmt="grid"))

        # Prompt user to select a dataset
        while True:
            try:
                selection = int(input("\nEnter the index of the dataset to select: "))
                if 1 <= selection <= len(datasets):
                    selected_dataset = datasets[selection - 1]
                    print(f"\nSelected Dataset: {selected_dataset}")
                    return selected_dataset
                else:
                    print("Invalid selection. Please choose a valid index.")
            except ValueError:
                print("Invalid input. Please enter a numeric index.")

    except subprocess.CalledProcessError as e:
        print("Error executing dasgoclient:", e)
        return None

if __name__ == "__main__":
    selected_dataset = query_datasets()
    if selected_dataset:
        print("\nYou can now proceed with:", selected_dataset)

