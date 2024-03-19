import os
import glob
import subprocess
from pathlib import Path
import datetime

runcom = subprocess.run
pjoin = os.path.join
PREFIX = "root://cmseos.fnal.gov"

def glob_files(dirname, startpattern='', endpattern='', **kwargs):
    """Returns a list of files matching a pattern in a directory. If both patterns are None, return all files.
    
    Parameters
    - `dirname`: directory path (remote/local)
    - `startpattern`: pattern to match the start of the file name
    - `endpattern`: pattern to match the end of the file name
    - `kwargs`: additional arguments for filtering files

    Return
    - A list of files (str)
    """
    if dirname.startswith('/store/user'):
        files = get_xrdfs_files(dirname, start_pattern=startpattern, end_pattern=endpattern, **kwargs)
    else:
        if startpattern == None and endpattern == None:
            files = [str(file.absolute()) for file in Path(dirname).iterdir() if file.is_file()]
        else:
            pattern = f'{startpattern}*{endpattern}'
            files = glob.glob(pjoin(dirname, pattern)) 
    return sorted(files)

def checkpath(dirname):
    """Check if a directory exists. If not will create one."""
    if dirname.startswith('/store/user/'):
        checkcondorpath(dirname)
    else:
        checklocalpath(dirname)

def transferfiles(srcpath, destpath, startpattern='', endpattern=''):
    """Transfer files between local and condor system.
    
    Parameters:
    - `srcpath`: source path (local/remote)
    - `destpath`: destination path (remote/local)
    - `startpattern`: pattern to match the start of the file name
    - `endpattern`: pattern to match the end of the file name
    """
    if isremote(destpath):
        if isremote(srcpath):
            raise ValueError("Source path should be a local directory. Why are you transferring from one EOS to another?")
        else:
            checkcondorpath(destpath)
            for srcfile in glob_files(srcpath, startpattern, endpattern):
                cpcondor(srcfile, f'{destpath}/{os.path.basename(srcfile)}', is_file=True)
    elif isremote(srcpath):
        if isremote(destpath):
            raise ValueError("Destination path should be a local directory. Why are you transferring from EOS to EOS?")
        else:
            checklocalpath(destpath)
            for srcfile in glob_files(srcpath, startpattern, endpattern):
                cpfcondor(srcfile, f'{destpath}/')

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
    
def checklocalpath(pathstr):
    """Check if a local path exists. If not will create one."""
    path = Path(pathstr) 
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

def cpfcondor(srcpath, localpath):
    """Copy a root file FROM condor to LOCAL."""
    comstr = f'xrdcp {srcpath} {localpath}' if srcpath.startswith('root://') else f'xrdcp {PREFIX}/{srcpath} {localpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    return result

def cpcondor(srcpath, destpath):
    """Copy srcpath (file/directory) FROM local to condor destpath"""
    comstr = f'xrdcp {srcpath} {destpath}' if destpath.startswith('root://') else f'xrdcp {srcpath} {PREFIX}/{destpath}' 
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    return result

def isremote(pathstr):
    """Check if a path is remote."""
    is_remote = pathstr.startswith('/store/user') or pathstr.startswith("root://")
    return is_remote

def checkcondorpath(dirname):
    """Check if a condor path exists. If not will create one."""
    check_dir_cmd = f"xrdfs {PREFIX} stat {dirname}"
    create_dir_cmd = f"xrdfs {PREFIX} mkdir -p {dirname}"
    proc = runcom(check_dir_cmd, shell=True, capture_output=True, text=True) 
    if proc.returncode != 0:
        runcom(create_dir_cmd, shell=True)
    else:
        return True

def delfiles(dirname, pattern='*.root'):
    """Delete all files in a directory with a specific pattern."""
    if pattern is not None:
        dirpath = Path(dirname)
        for fipath in dirpath.glob(pattern):
            fipath.unlink()

def get_xrdfs_files(remote_dir, start_pattern, end_pattern, add_prefix=True):
    """Get the files in a remote directory that match a pattern. If both patterns==None, returns all files.
    
    Parameters:
    - `remote_dir`: remote directory path
    - `start_pattern`: pattern to match the start of the file name
    - `end_pattern`: pattern to match the end of the file name
    - `add_prefix`: if True, will add the PREFIX to the file path
    
    Returns:
    - list of files that match the pattern
    """
    all_files = list_xrdfs_files(remote_dir)
    if start_pattern == None and end_pattern == None:
        return sorted(all_files)
    else:
        if add_prefix:
            filtered_files = [PREFIX + "/" + f for f in all_files if f.split('/')[-1].startswith(start_pattern) and f.split('/')[-1].endswith(end_pattern)]
        else: 
            filtered_files = [f for f in all_files if f.split('/')[-1].startswith(start_pattern) and f.split('/')[-1].endswith(end_pattern)]
        return sorted(filtered_files)

def list_xrdfs_files(remote_dir):
    """List files/dirs in a remote xrdfs directory using subprocess.run."""
    cmd = ["xrdfs", PREFIX, "ls", remote_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = sorted(result.stdout.strip().split('\n'))
    return files

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
