import subprocess
from rich.console import Console
from rich.table import Table

runcom = subprocess.run

def query_datasets():
    """
    Prompts the user for dataset search parameters, queries DAS using dasgoclient,
    and returns matching datasets.

    Returns:
    - list: A list of matching dataset names.
    """
    
    # Prompt user for input
    prefix = input("Enter dataset name prefix (e.g., TTToSemiLeptonic): ").strip()
    year = input("Enter year (e.g., Summer23): ").strip()
    nanoaod_version = input("Enter NANOAOD version (or press enter to skip): ").strip()

    print("Select data tier: 1. NANOAOD 2. MINIAOD")
    data_tier_index = input("Enter the index of the data tier (1 or 2): ").strip()

    # Validate input
    if not prefix or not year:
        print("Prefix and year are required fields.")
        return []

    if data_tier_index == "1":
        data_tier = "NANOAOD"
    elif data_tier_index == "2":
        data_tier = "MINIAOD"
    else:
        print("Invalid selection! Please enter '1' for NANOAOD or '2' for MINIAOD.")
        return []

    # Construct DAS query
    query = f"dataset=/{prefix}*/*{year}*/{data_tier}*"
    if nanoaod_version:
        query = f"dataset=/{prefix}*/*{year}*v{nanoaod_version}*/{data_tier}*"

    print("\nRunning DAS query:", query)

    try:
        result = runcom(f'dasgoclient -query="{query}"', shell=True, capture_output=True, text=True)
        datasets = result.stdout.strip().split("\n")
        datasets = [ds for ds in datasets if ds]

        if not datasets:
            print("\nNo matching datasets found.")
            return None
        
        console = Console()

        table = Table(title="Matching Datasets", show_lines=True)

        table.add_column("Index", style="bold cyan")
        table.add_column("Dataset Name", style="bold yellow")

        for i, ds in enumerate(datasets, 1):
            table.add_row(str(i), ds)

        console.print(table)

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

