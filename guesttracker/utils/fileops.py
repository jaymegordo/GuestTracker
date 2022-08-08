import os
import shutil
# import signal
import subprocess
import sys
import time
from datetime import datetime as dt
from io import StringIO
from pathlib import Path
from typing import *
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd

from guesttracker import config as cf
from guesttracker import errors as er
from guesttracker import functions as f
from guesttracker import getlog

# import psutil


log = getlog(__name__)


def get_parent(p: Path, name: str, offset: int = 0) -> Path:
    """Get Path parent by name
    - protects for max/min of parents length

    Parameters
    ----------
    p : Path
    name : str
        parent name to find
    offset : int
        offset from found parent

    Returns
    -------
    Path
        parent of original Path
    """
    parents = p.parents
    m = {k.name.lower(): i for i, k in enumerate(parents)}
    i = min(max(m[name.lower()] - offset, 0), len(parents) - 1)
    return p.parents[i]


def date_created(p) -> dt:
    """Get date from folder date created (platform dependent)

    Parameters
    ----------
    p : Path
        Folder path to check
    """
    st = p.stat()
    ts = st.st_ctime if cf.is_win else st.st_birthtime

    return dt.fromtimestamp(ts)


def date_modified(p):
    return dt.fromtimestamp(p.stat().st_mtime)


def delete_folder(p):
    shutil.rmtree(p)


def copy_folder_contents(p_src: Path, p_dst: Path, symlinks: bool = False, ignore=None) -> None:
    """Copy contents of source folder to dest folder"""
    for item in os.listdir(p_src):
        s = os.path.join(p_src, item)
        d = os.path.join(p_dst, item)

        if os.path.isdir(s):
            try:
                shutil.copytree(s, d, symlinks, ignore)
            except FileExistsError as e:
                log.warning(f'Dest folder exists, can\'t copy\n\t{e}')
        else:
            shutil.copy2(s, d)


def move_folder(p_src: Path, p_dst: Path) -> None:
    """Move contents of source folder, or file from p_src to p_dst"""
    try:
        if p_src.exists() and not p_src == p_dst:
            src, dst = str(p_src), str(p_dst)

            # if dest folder already exists, need to copy then remove
            if p_dst.exists() and p_src.is_dir():

                copy_folder_contents(p_src, p_dst)
                shutil.rmtree(src)
            else:
                shutil.move(src, dst)

    except Exception as e:
        log.error(f'Failed copying folder: {p_src}')
        raise e


def copy_file(p_src, p_dst, overwrite=False):
    p = p_dst.parent
    if not p.exists():
        p.mkdir(parents=True)

    if not p_dst.exists() or overwrite:
        shutil.copyfile(str(p_src), str(p_dst))
    else:
        print(f'File already exists: {p_dst.name}')


@er.errlog(msg='Error zipping folder', warn=True, display=True)
def zip_folder_threadsafe(
        p_src: Path,
        p_dst: Path = None,
        delete: bool = False) -> Union[Path, None]:
    """zip target file/folder in place, optional delete original

    Parameters
    ----------
    p_src : Path
        Source file/folder to zip
    p_dst : Path, optional
        destination file/folder, default None
    delete : bool, optional
        delete file/folder after zip, default False

    Returns
    -------
    Union[Path, None]
        Path if zipped successfully, else None
    """
    log.info(f'zip_folder_threadsafe: {p_src}')

    if p_dst is None:
        p_dst = p_src
    else:
        if not p_dst.parent.exists():
            p_dst.parent.mkdir(parents=True)

            # set dst to dst folder + source file name
            # p_dst = p_dst / p_src.stem

    if not p_src.exists():
        log.warning(f'Can\'t zip file, doesn\'t exist: {p_src}')
        return

    p_dst = f'{p_dst}.zip'

    with ZipFile(p_dst, mode='w', compression=ZIP_DEFLATED) as zf:
        paths = [Path(p_src)]

        while paths:
            p = paths.pop()
            if p.is_dir():
                paths.extend(p.iterdir())

            # need relative path for arcname
            zf.write(filename=p, arcname=os.path.relpath(p, p_src))

    if delete:
        shutil.rmtree(p_src)

    return Path(p_dst)


# @er.errlog(msg='Error zipping folder', warn=True, display=True)
def zip_folder(
        p: Path,
        p_dst: Path = None,
        delete: bool = False,
        calculate_size: bool = False) -> Union[Path, None]:
    """zip target file/folder in place, optional delete original

    Parameters
    ----------
    p : Path
        Source file/folder to zip
    p_dst : Path, optional
        destination file/folder, default None
    delete : bool, optional
        delete file/folder after zip, default False
    calculate_size : bool, optional
        calculate and print file compression savings, default False

    Returns
    -------
    Union[Path, None]
        Path if zipped successfully, else None
    """
    log.info(f'zip_folder: {p}')

    # zip folder into a new target dir
    # if p_dst is none, just zip in place
    if p_dst is None:
        p_dst = p
    else:
        if p_dst.is_dir():
            if not p_dst.exists():
                p_dst.mkdir(parents=True)

            # set dst to dst folder + source file name
            p_dst = p_dst / p.stem

    if not p.exists():
        log.warning(f'Can\'t zip file, doesn\'t exist: {p}')
        return

    p_zip = shutil.make_archive(
        base_name=str(p_dst),
        base_dir=p.name,
        root_dir=str(p.parent),
        format='zip')

    # print file size compression savings
    if calculate_size:
        size_pre = calc_size(p=p, as_str=False)
        size_post = sum(f.stat().st_size for f in p_dst.parent.glob('*.zip'))
        size_pct = size_post / size_pre
        print(f'Reduced size to: {size_pct:.1%}\nPre: {size_readable(size_pre)}\nPost: {size_readable(size_post)}')

    if delete:
        try:
            shutil.rmtree(p)
        except Exception as e:
            log.error(f'Failed to delete folder: {p}')

    return Path(p_zip)


def unzip(p: Path, p_dst: Path = None, delete=False) -> Path:
    """
    TODO delete this eventually... moved to jgutils
    Simple wrapper for shultil unpack_archive with default unzip dir

    Parameters
    ----------
    p : Path
        File to unzip
    p_dst : Path, optional
        Unzip in different dir, by default parent dir
    delete : bool, optional
        Delete original zip after unpack, by default False
    """
    if p_dst is None:
        p_dst = p.parent

    shutil.unpack_archive(p, p_dst)

    if delete:
        p.unlink()

    return p


def unzip_pyu_archive():
    """Win only, convenience func to unzip pyu archive to local dir after build"""
    p = cf.projectfolder / 'pyu-data/files'
    p = [p for p in p.glob('*zip*')][0]
    p_dst = Path.home() / 'documents/guesttracker_pyu'
    return unzip(p=p, p_dst=p_dst)


def remove_files(lst):
    for p in lst:
        if p.exists():
            p.unlink()


def count_files(p, extensions=None, ftype='pics'):
    if ftype.lower() == 'pics':
        extensions = ['jpeg', 'jpg', 'png', 'tiff', 'heic']

    return len(find_files_ext(p=p, extensions=extensions))


def find_files_ext(p, extensions):
    extensions = f.as_list(extensions)
    return [p_ for p_ in p.rglob('*') if p_.suffix.lower().replace('.', '') in extensions and len(p_.suffix) > 0]


def find_files_partial(p, partial_text, recursive=False):
    func = 'glob' if not recursive else 'rglob'

    # regex pattern match option
    # pattern = re.compile('serial', re.IGNORECASE)
    # return [p_ for p_ in p.rglob('*.pdf') if re.search(pattern, str(p_))]

    return [p_ for p_ in getattr(p, func)('*') if partial_text.lower() in str(p_).lower()]


def size_readable(nbytes):
    """Return human readable file size string from bytes"""
    suffixes = ('B', 'KB', 'MB', 'GB', 'TB', 'PB')
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1

    f = f'{nbytes:.2f}'.rstrip('0').rstrip('.')
    return f'{f} {suffixes[i]}'


def calc_size(p: Path, as_str: bool = True):
    """Calculate size of directory and all subdirs

    Parameters
    ----------
    p : Path
    as_str : bool, optional
        return raw float or nicely formatted string, default False

    Returns
    -------
    int | string
        size of folder
    """
    _size = sum(f.stat().st_size for f in p.glob('**/*') if f.is_file())
    return size_readable(_size) if as_str else _size

# OTHER


def check(p: Path, **kw):
    """Check path exists, with drive check first"""
    if not isinstance(p, Path):
        p = Path(p)

    # if path is on drive, manual check drive first
    if cf.p_drive.as_posix() in p.as_posix():
        if not drive_exists(**kw):
            return False
        else:
            return p.exists()
    else:
        return p.exists()


def drive_exists(warn: bool = True, timeout: int = 2) -> bool:
    """Check if network drive exists

    Parameters
    ----------
    warn : bool, optional
        raise NoPDriver error or not, default True\n
    timeout : int, optional
        timeout in seconds to ping drive (windows only), default 2\n

    Returns
    -------
    bool
        if drive exists

    Raises
    ------
    er.NoPDriveError
        (er.ExpectedError), will just stop process and warn but not break anything
    """
    _exists = False
    if cf.is_win:
        # path.exists() takes 20s to check on win if doesnt exists, use ping instead (kinda sketch)
        ip = '172.17.1.163'
        args = f'-n 1 -w {timeout * 1000}'
        cmd = f'ping {args} {ip}'
        _exists = not subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW).returncode
    else:
        _exists = cf.p_drive.exists()

    if _exists:
        return True
    elif warn:
        raise er.NoPDriveError()
    else:
        return False


def open_folder(p: Path, check_drive: bool = False) -> None:
    """Open folder in Finder/File Explorer

    Parameters
    ----------
    p : Path
        file path to open to
    check_drive : bool, optional
        check if p drive connected first, by default False
    """
    if check_drive and not drive_exists():
        return

    if isinstance(p, str):
        p = Path(p)

    if not p.exists():
        return

    platform = sys.platform
    if platform.startswith('win'):
        os.startfile(p)
    elif platform.startswith('dar'):
        # open finder window, zoom, bring to front
        if p.is_dir():
            args = [
                'osascript',
                '-e tell application "Finder"',
                f'-e open ("{p}" as POSIX file)',
                '-e tell window 1',
                '-e set zoomed to true',
                '-e end tell',
                '-e activate',
                '-e end tell']

            subprocess.run(args, stdout=subprocess.DEVNULL)
        else:
            subprocess.Popen(['open', p])
    else:
        subprocess.Popen(['xdg-open', p])


def get_app_name(appNamesList, app):
    for appName in appNamesList:
        if app in appName:
            return appName
    return ''


def open_app(name):
    # p = subprocess.Popen(["ls",  "/Applications/"], stdout=subprocess.PIPE)
    # appNames = p.communicate()[0].split('\n')
    # appName = get_app_name(appNames, name)

    if not name == '':
        p = subprocess.Popen(['open', '-n', '/Applications/' + name], stdout=subprocess.PIPE)
    else:
        print('No app with that name installed')


def tree(directory):
    # TODO: maybe move this to functions
    print(f'+ {directory}')
    for path in sorted(directory.rglob('*')):
        depth = len(path.relative_to(directory).parts)
        spacer = '    ' * depth
        print(f'{spacer}+ {path.name}')


def from_bytes(bytes):
    """Return string from bytes object
    - Useful for reading csv/excel data from bytes so far"""
    result = str(bytes, 'UTF-8')
    return StringIO(result)


def read_access_database(p: Path, table_name: str, index_col: str = None, raw_data: bool = False):
    """Read table from access database to df
    - NOTE needs 'mdb-export' installed (mac only)
    - TODO maybe make this work on windows

    Parameters
    ----------
    p : Path
        path to .accdb
    table_name : str
        table in database to read
    index_col : str, optional
        optional index col, by default None
    raw_data : bool, optional
        optional return raw data to pass extra args to read_csv

    Returns
    -------
    pd.DataFrame | raw data
    """
    cmd = ['mdb-export', p.as_posix(), table_name]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    data = from_bytes(proc.stdout.read())  # stdout returns io.BufferedReader obj

    if raw_data:
        return data

    return pd.read_csv(data, index_col=index_col) \
        .pipe(f.default_data)


# def find_procs_by_name(name):
#     "Return a list of processes matching 'name'."
#     return [p for p in psutil.process_iter(['name']) if name.lower() in p.info['name'].lower()]


def kill_proc(procs):
    """Try to kill locking sms proc
    - Used to stop background processes getting stuck locking the update file"""
    for p in procs:
        try:
            p.kill()
        except Exception as e:
            log.warning(f'Failed to kill process: {p.name()}')


# def get_sms_proc_locking(filename='zip'):
#     """Get background HBA Guest Tracker.exe process if locking 'zip' file for update"""
#     procs = find_procs_by_name('sms event log')
#     pid_self = os.getpid()

#     # check if each process has any open files containing search filename
#     return [p for p in procs if
#             not p.pid == pid_self and
#             [x for x in [item.path for item in p.open_files()] if x.endswith(filename)]]


# def kill_sms_proc_locking(filename='zip'):
#     procs = get_sms_proc_locking(filename)
#     num_procs = len(procs)

#     log.info(f'Found {num_procs} locking processes.')
#     if num_procs == 1:
#         proc = procs[0]
#         try:
#             log.info(f'Trying to kill process: {proc.name()}, {proc.pid}')
#             return kill_proc_tree(pid=proc.pid)
#         except Exception as e:
#             log.error('Failed to kill locking process!')
#     elif num_procs > 1:
#         # will need to figure this out
#         log.error('Too many locking procs found to delete!')


# def kill_proc_tree(pid, sig=signal.SIGTERM, include_parent=True,
#                    timeout=None, on_terminate=None):
#     """Kill a process tree (including grandchildren) with signal
#     "sig" and return a (gone, still_alive) tuple.
#     "on_terminate", if specified, is a callabck function which is
#     called as soon as a child terminates.
#     """
#     assert pid != os.getpid(), "won't kill myself"

#     parent = psutil.Process(pid)
#     children = parent.children(recursive=True)

#     if include_parent:
#         children.append(parent)

#     for p in children:
#         p.send_signal(sig)

#     gone, alive = psutil.wait_procs(children, timeout=timeout,
#                                     callback=on_terminate)
#     return (gone, alive)


def find_unit_sap(unit: str):
    """AppleScript to automate finding unit with cmd+f in citrix sap

    Parameters
    ----------
    unit : str
        unit to find eg 'F0305'
    """
    if cf.is_win:
        raise er.SMSNotImplementedError()

    from aeosa.appscript import app, k
    delay = 0.5

    citrix = app('Citrix Viewer')
    syst = app('System Events')

    citrix.activate()

    time.sleep(delay)
    syst.click()  # click so sap is 'activated' better..
    # time.sleep(delay)
    syst.keystroke('f', using=k.command_down)
    time.sleep(delay)
    syst.keystroke(unit)
    time.sleep(delay)
    syst.keystroke('\r')
    time.sleep(delay)
    syst.key_code(53)
