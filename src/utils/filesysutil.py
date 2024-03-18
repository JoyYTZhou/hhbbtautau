import os
import glob
import subprocess
from pathlib import Path

runcom = subprocess.run
pjoin = os.path.join
PREFIX = "root://cmseos.fnal.gov"


def list_xrdfs_files(remote_dir):
    """List files/dirs in a remote xrdfs directory using subprocess.run."""
    cmd = ["xrdfs", PREFIX, "ls", remote_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = sorted(result.stdout.strip().split('\n'))
    return files

def glob_files(dirname, startpattern, endpattern, **kwargs):
    """Returns a list of files matching a pattern in a directory.
    
    Parameters
    - `dirname`: directory path (remote/local)
    - `startpattern`: pattern to match the start of the file name
    - `endpattern`: pattern to match the end of the file name
    - `kwargs`: additional arguments for filtering files
    """
    if dirname.startswith('/store/user'):
        files = filter_xrdfs_files(dirname, start_pattern=startpattern, end_pattern=endpattern, **kwargs)
    else:
        pattern = f'{startpattern}*{endpattern}'
        files = glob.glob(pjoin(dirname, pattern)) 
    return sorted(files)

def checkcondorpath(dirname):
    """Check if a condor path exists. If not will create one."""
    check_dir_cmd = f"xrdfs {PREFIX} stat {dirname}"
    create_dir_cmd = f"xrdfs {PREFIX} mkdir -p {dirname}"

    proc = runcom(check_dir_cmd, shell=True, capture_output=True, text=True) 

    if proc.returncode != 0:
        runcom(create_dir_cmd, shell=True)
    else:
        return True

def delfilelist(filelist):
    """Remove a list of file"""
    for file_path in filelist:
        file = Path(file_path)
        try:
            file.unlink()
            print(f"Deleted {file}")
        except FileNotFoundError:
            print(f"File not found: {file}")
        except PermissionError:
            print(f"Permission denied: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")
    return None
    
def checkpath(pathstr):
    """Check if a local path exists. If not will create one."""
    path = Path(pathstr) 
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

def cpfcondor(srcpath, localpath):
    """Copy a root file FROM condor to LOCAL."""
    comstr = f'xrdcp {srcpath} {localpath}' if srcpath.startswith('root://') else f'xrdcp {PREFIX}/{srcpath} {localpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    return result

def cpcondor(srcpath, destpath, is_file=True):
    """Copy srcpath (file/directory) FROM local to condor destpath"""
    comstr = f'xrdcp -f {srcpath} {PREFIX}/{destpath}' if is_file else f'xrdcp -r {srcpath} {destpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    return result

def transferfiles(srcpath, destpath):
    """Copy all files FROM one local directory to a condor directory. 
    If destpath does not exist, will create one."""
    checkcondorpath(destpath)
    for srcfile in Path(srcpath).iterdir():
        if srcfile.is_file():
            cpcondor(str(srcfile), f"{destpath}/{srcfile.name}", is_file=True)


def delfiles(dirname, pattern='*.root'):
    """Delete all files in a directory with a specific pattern."""
    if pattern is not None:
        dirpath = Path(dirname)
        for fipath in dirpath.glob(pattern):
            fipath.unlink()

def filter_xrdfs_files(remote_dir, start_pattern, end_pattern, add_prefix=True):
    """Filter XRDFS files in a remote directory by a specific file ending."""
    all_files = list_xrdfs_files(remote_dir)
    if add_prefix:
        filtered_files = [PREFIX + "/" + f for f in all_files if f.split('/')[-1].startswith(start_pattern) and f.split('/')[-1].endswith(end_pattern)]
    else: 
        filtered_files = [f for f in all_files if f.split('/')[-1].startswith(start_pattern) and f.split('/')[-1].endswith(end_pattern)]
    return sorted(filtered_files)

def get_xrdfs_file_info(remote_file):
    """Get information (size, modification time) of a remote xrdfs file/dir"""
    cmd = ["xrdfs", PREFIX, "stat", remote_file]
    output = subprocess.check_output(cmd).decode()
    size = None
    mod_time = None

    for line in output.split('\n'):
        if line.startswith('Size:'):
            size = int(line.split()[1])
        elif line.startswith('MTime:'):
            mod_time = ' '.join(line.split()[1:])
    return size, mod_time

def sync_files(local_dir, remote_dir):
    """Check for discrepancies and update local files from a remote xrdfs directory."""
    remote_files = list_xrdfs_files(remote_dir)
    for remote_file in remote_files:
        local_file = os.path.join(local_dir, os.path.basename(remote_file))
        if not os.path.exists(local_file):
            copy_file = True
        else:
            local_size = os.path.getsize(local_file)
            local_mod_time = datetime.fromtimestamp(os.path.getmtime(local_file))
            remote_size, remote_mod_time_str = get_xrdfs_file_info(remote_file)
            remote_mod_time = datetime.strptime(remote_mod_time_str, '%Y-%m-%d %H:%M:%S')
            copy_file = (local_size != remote_size) or (local_mod_time < remote_mod_time)
        
        if copy_file:
            cmd = ["xrdcp", f"{PREFIX}/{remote_file}", local_file]
            subprocess.run(cmd)
