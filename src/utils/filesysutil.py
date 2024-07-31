import os, glob, tracemalloc, linecache, subprocess, datetime, fnmatch
from pathlib import Path

runcom = subprocess.run
pjoin = os.path.join
PREFIX = "root://cmseos.fnal.gov"
DEBUG_ON = os.environ.get("DEBUG_MODE", default=False)

def glob_files(dirname, filepattern='*', **kwargs) -> list:
    """Returns a SORTED list of files matching a pattern in a directory. If both patterns are None, return all files.
    
    Parameters
    - `dirname`: directory path (remote/local)
    - `filepattern`: pattern to match the file name
    - `kwargs`: additional arguments for filtering files

    Return
    - A SORTED list of files (str)
    """
    if dirname.startswith('/store/user'):
        files = get_xrdfs_files(dirname, filepattern, **kwargs)
    else:
        if filepattern == '*':
            files = [str(file.absolute()) for file in Path(dirname).iterdir() if file.is_file()]
        else:
            files = glob.glob(pjoin(dirname, filepattern)) 
    return sorted(files)

def checkpath(dirname, createdir=True, raiseError=False):
    """Check if a directory exists. If not will create one."""
    if dirname.startswith('/store/user/'):
        return checkcondorpath(dirname, createdir, raiseError=raiseError)
    else:
        return checklocalpath(dirname, raiseError)

def transferfiles(srcpath, destpath, startpattern='', endpattern='', remove=False):
    """Transfer files between local and condor system. Will check if destpath exist.
    
    Parameters:
    - `srcpath`: source path (local/remote)
    - `destpath`: destination path (remote/local)
    - `startpattern`: pattern to match the start of the file name
    - `endpattern`: pattern to match the end of the file name
    - `remove`: mv/cp
    """
    if isremote(destpath):
        if isremote(srcpath):
            raise ValueError("Source path should be a local directory. Why are you transferring from one EOS to another?")
        else:
            checkcondorpath(destpath)
            for srcfile in glob_files(srcpath, startpattern, endpattern):
                cpcondor(srcfile, f'{destpath}/{os.path.basename(srcfile)}')
                if remove: 
                    os.remove(srcfile)
    elif isremote(srcpath):
        if isremote(destpath):
            raise ValueError("Destination path should be a local directory. Why are you transferring from EOS to EOS?")
        else:
            checklocalpath(destpath)
            for srcfile in glob_files(srcpath, startpattern, endpattern):
                cpfcondor(srcfile, f'{destpath}/')

def checkx509():
    """Check if the X509 proxy and certificate directory are set."""
    proxy_position = os.environ.get("X509_USER_PROXY", default="None")
    if proxy_position is None:
        raise SystemError("Proxy not found. Immediately check proxy!")
    print(f"Proxy at: {proxy_position}.")
    proxy_directory = os.environ.get("X509_CERT_DIR", default="None")
    if proxy_directory is None:
        raise SystemError("Certificate directory not set. Immmediately check certificate directory!")
    print(f"Certificate directory set to be {proxy_directory}.")

def display_top(snapshot, key_type='lineno', limit=10):
    """Display the top lines of a snapshot"""
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        print("#%s: %s:%s: %.1f KiB"
              % (index, frame.filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))
    
def checklocalpath(pathstr, raiseError=False):
    """Check if a local path exists. If not will create one."""
    path = Path(pathstr) 
    if not path.exists():
        if raiseError:
            raise FileNotFoundError(f"this file {pathstr} does not exist.")
        else:
            path.mkdir(parents=True, exist_ok=True)

def cpfcondor(srcpath, localpath):
    """Copy a root file FROM condor to LOCAL."""
    comstr = f'xrdcp {srcpath} {localpath}' if srcpath.startswith('root://') else f'xrdcp {PREFIX}/{srcpath} {localpath}'
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    if DEBUG_ON and result.returncode !=0 : print(result.stderr)
    return result

def cpcondor(srcpath, destpath):
    """Copy srcpath (file/directory) FROM local to condor destpath"""
    comstr = f'xrdcp {srcpath} {destpath}' if destpath.startswith('root://') else f'xrdcp {srcpath} {PREFIX}/{destpath}' 
    result = runcom(comstr, shell=True, capture_output=True, text=True)
    if DEBUG_ON and result.returncode !=0 : print(result.stderr)
    return result

def isremote(pathstr):
    """Check if a path is remote."""
    is_remote = pathstr.startswith('/store/user') or pathstr.startswith("root://")
    return is_remote

def check_missing(pattern, fileno, dirtocheck, endpattern='.root', return_indices=True):
    """Check missing output files in a directory based on pattern search. The output files in the directory are
    expected to have names like ${pattern}_${i}, where i is the index of the file.
    
    Return:
    - list of missing files if return_indices=False. Otherwise, return list of missing indices."""
    existfiles = glob_files(dirtocheck, startpattern=pattern, endpattern=endpattern)
    fileset = set(existfiles)
    patterns = [f"{pattern}_{i}" for i in range(fileno)]
    if return_indices: 
        toreturn = [i for i, pattern in enumerate(patterns) if not any(pattern in file for file in fileset)]
    else:
        toreturn = [pattern for i, pattern in enumerate(patterns) if not any(pattern in file for file in fileset)] 
    return toreturn

def checkcondorpath(dirname, createdir=True, raiseError=False):
    """Check if a condor path exists and potentially create one.
    Parameters:
    `dirname`: directory path to check
    `createdir`: if True, create the directory if it doesn't exist. If false, return stat code. 
    `raiseError`: if True, raise an error if the directory doesn't exist."""
    check_dir_cmd = f"xrdfs {PREFIX} stat {dirname}"
    create_dir_cmd = f"xrdfs {PREFIX} mkdir -p {dirname}"
    proc = runcom(check_dir_cmd, shell=True, capture_output=True, text=True, check=raiseError) 
    if proc.returncode != 0:
        if createdir:
            runcom(create_dir_cmd, shell=True)
        else:
            return proc.returncode
    else:
        return 0

def delfiles(dirname, pattern='*.root'):
    """Delete all files in a directory with a specific pattern."""
    if pattern is not None:
        dirpath = Path(dirname)
        for fipath in dirpath.glob(pattern):
            fipath.unlink()

def get_xrdfs_files(remote_dir, filepattern='*', add_prefix=True) -> list[str]:
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
    if filepattern == '*':
        return sorted(all_files)
    else:
        if add_prefix:
            filtered_files = [PREFIX + "/" + f for f in all_files if fnmatch.fnmatch(f.split('/')[-1], filepattern)]
        else: 
            filtered_files = [f for f in all_files if fnmatch.fnmatch(f.split('/')[-1], filepattern)]
        return sorted(filtered_files)

def list_xrdfs_files(remote_dir) -> list[str]:
    """List files/dirs in a remote xrdfs directory using subprocess.run."""
    cmd = ["xrdfs", PREFIX, "ls", remote_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = sorted(result.stdout.strip().split('\n'))
    return files

def get_xrdfs_file_info(remote_file, redir=PREFIX):
    """Get information (size, modification time) of a remote xrdfs file/dir.
    
    Parameters
    ``remote_file``: remote file path
    Returns
    - size of the file in bytes (int)
    - modification time of the file (str)"""
    cmd = ["xrdfs", redir, "stat", remote_file]
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