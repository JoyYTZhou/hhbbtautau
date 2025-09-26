import os, logging, glob
import pandas as pd
from rich.table import Table
from rich.console import Console
from rich.prompt import Prompt, Confirm
from src.plotting.visutil import CSVPlotter
from src.utils.ioutil import setup_logging
from src.utils.displayutil import RichArgumentParser
import matplotlib

pjoin = os.path.join
matplotlib.use('Agg')

def interactive_file_selection(directory=".", required_count=None) -> list:
    """Interactive file selection using rich."""
    console = Console()
    
    # Find CSV files recursively
    csv_files = glob.glob(os.path.join(directory, "**/*.csv"), recursive=True)
    
    if not csv_files:
        console.print("[red]No CSV files found in the specified directory![/red]")
        return []
    
    # Display files in a table
    table = Table(title=f"Available CSV Files in {os.path.abspath(directory)}")
    table.add_column("Index", style="cyan", justify="center")
    table.add_column("Filename", style="green")
    table.add_column("Size", style="yellow", justify="right")
    table.add_column("Modified", style="blue")
    
    for i, file_path in enumerate(csv_files):
        try:
            stat = os.stat(file_path)
            size = f"{stat.st_size / 1024:.1f} KB"
            modified = pd.Timestamp(stat.st_mtime, unit='s').strftime('%Y-%m-%d %H:%M')
        except OSError:
            size = "N/A"
            modified = "N/A"
        
        # Show relative path if it's shorter
        display_path = os.path.relpath(file_path, directory)
        if len(display_path) > 60:
            display_path = "..." + display_path[-57:]
        
        table.add_row(str(i), display_path, size, modified)
    
    console.print(table)
    
    # Get user selection
    if required_count:
        console.print(f"\n[bold]Please select exactly {required_count} files.[/bold]")
    else:
        console.print("\n[bold]Select files by entering their indices.[/bold]")
    
    while True:
        try:
            selection_input = Prompt.ask(
                "Enter file indices (comma-separated, e.g., 0,1,2)",
                default=""
            )
            
            if not selection_input.strip():
                if Confirm.ask("No files selected. Do you want to exit?"):
                    return []
                continue
            
            # Parse selection
            indices = [int(i.strip()) for i in selection_input.split(',') if i.strip().isdigit()]
            
            # Validate indices
            invalid_indices = [i for i in indices if i < 0 or i >= len(csv_files)]
            if invalid_indices:
                console.print(f"[red]Invalid indices: {invalid_indices}. Please use indices 0-{len(csv_files)-1}.[/red]")
                continue
            
            # Check required count
            if required_count and len(indices) != required_count:
                console.print(f"[red]Please select exactly {required_count} files. You selected {len(indices)}.[/red]")
                continue
            
            # Confirm selection
            selected_files = [csv_files[i] for i in indices]
            console.print("\n[bold]Selected files:[/bold]")
            for i, file_path in enumerate(selected_files):
                console.print(f"  {i+1}. {os.path.relpath(file_path, directory)}")
            
            if Confirm.ask("\nConfirm selection?", default=True):
                return selected_files
            
        except ValueError:
            console.print("[red]Invalid input. Please enter comma-separated numbers.[/red]")
        except KeyboardInterrupt:
            console.print("\n[yellow]Selection cancelled.[/yellow]")
            return []

def plot_Rwgt_DataMinusMC(src_df, rwgt_df, out_dir):
    from config.plotsetting import H_mass, tau_pt, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt |bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    
    oneD_df = src_df.copy()
    compare_df = src_df[src_df['group'] == 'Data'].copy()
    oneD_df.loc[oneD_df['group'] != 'Data', 'weight'] *= -1
    rwgt_df['weight'] = rwgt_df['weight_reco_p3'].copy()
    compare_df = compare_df[compare_df['group'] == 'Data'].copy()
    renorm_fac = oneD_df['weight'].sum() / compare_df['weight'].sum()
    compare_df['weight'] *= renorm_fac

    logging.info("Plotting 1D subtraction vs multi-D reweighting results.")
    logging.info(f"Number of events in QCD: {oneD_df['weight'].sum()}")

    cp.plot_shape([oneD_df, rwgt_df, compare_df], labels=['1D subtraction', 'multi-D reweighting', 'Total Data'], 
                  attridict=att_dicts, ratio_ylabel='Pred/Actual', outdir=out_dir,
                  normalize=False, title='Multijet Background', save_suffix='QCD')
    
def plot_Rwgt_SSvsOS(ss_df, os_df, out_dir):
    from config.plotsetting import H_mass, tau_pt, tau_eta, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt | tau_eta | bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    ss_df = ss_df[ss_df['group'] == 'Data']
    reco_os_df = ss_df.copy()
    reco_os_df['weight'] = ss_df['weight_reco_os'].copy()
    os_df = os_df[os_df['group'] == 'Data']
    renorm_fac = os_df['weight'].sum() / ss_df['weight'].sum()
    ss_df['weight'] *= renorm_fac
    logging.info("Plotting SS reweighted to OS vs actual OS distributions.")
    cp.plot_shape([os_df, ss_df, reco_os_df], labels=['OS data', 'SS data', 'SS reweighted to OS'], 
                  attridict=att_dicts, ratio_ylabel='Pred/Actual', outdir=out_dir,
                  normalize=False, title='Total Background', save_suffix='SSvsOS')

def compare(df1, df2, out_dir, df1_label, df2_label):
    from config.plotsetting import H_mass, tau_pt, tau_eta, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt | tau_eta | bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    logging.info("Comparing two datasets.")
    df1[df1['group'] != 'Data'].weight *= -1
    logging.info(f"Number of events in {df1_label}: {df1['weight'].sum()}")
    df2[df2['group'] != 'Data'].weight *= -1
    logging.info(f"Number of events in {df2_label}: {df2['weight'].sum()}")
    cp.plot_shape([df1, df2], labels=[df1_label, df2_label], 
                  attridict=att_dicts, ratio_ylabel='ratio', outdir=out_dir,
                  normalize=True, title='Dataset Comparison', save_suffix='Compare')
    
if __name__ == "__main__":
    setup_logging()
    program_description = """Script to generate plots for different analysis methods."""
    parser = RichArgumentParser(description=program_description)
    mode_description = """Mode of operation for the script. 
    DataMinusMC: Compare 1D subtration versus MD subtraction via NN density estimation.
    OriVSRwgt: Compare original versus reweighted distributions."""

    parser.add_argument("mode", choices=['DataMinusMC', 'OriVSRwgt', 'Compare'], help=mode_description)
    parser.add_argument("-i", "--input", nargs='+', required=True, help="Input directory(ies) containing CSV files")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    parser.add_argument("-l", "--labels", nargs='+', required=False, help="Labels for the datasets")
    
    args = parser.parse_args()

    input_files = []
    for input_path in args.input:
        if not os.path.exists(input_path):
            logging.error(f"Input path does not exist: {input_path}")
            exit(1)
        if not os.path.isdir(input_path):
            logging.error(f"Input path is not a directory: {input_path}")
            exit(1)
        input_files.extend(interactive_file_selection(directory=input_path))

    if args.mode == "DataMinusMC":
        if len(input_files) == 2:
            src_df = pd.read_csv(input_files[0])
            tar_df = pd.read_csv(input_files[1])
            plot_Rwgt_DataMinusMC(src_df, tar_df, args.output)
        else:
            logging.error("DataMinusMC mode requires exactly 2 input CSV files.")
            exit(1)

    elif args.mode == "OriVSRwgt":
        if len(input_files) == 2:
            ss_df = pd.read_csv(input_files[0])
            os_df = pd.read_csv(input_files[1])
            plot_Rwgt_SSvsOS(ss_df, os_df, args.output)
        else:
            logging.error("OriVSRwgt mode requires exactly 2 input CSV files.")
            exit(1)

    elif args.mode == "Compare":
        if len(input_files) == 2 and args.labels and len(args.labels) == 2:
            df1 = pd.read_csv(input_files[0])
            df2 = pd.read_csv(input_files[1])
            compare(df1, df2, args.output, args.labels[0], args.labels[1])
        else:
            logging.error("Compare mode requires exactly 2 input CSV files and 2 labels.")
            exit(1)
