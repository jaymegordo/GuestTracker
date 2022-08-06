# -*- mode: python -*-
# type: ignore

project_path = None

import importlib
import os
import sys
import time
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules
from PyInstaller.building.build_main import TOC

start = time.time()

import warnings

warnings.filterwarnings(
    action='ignore',
    category=SyntaxWarning,
    module=r'.*firefox_profile')

# NOTE hookspath and project_path will need to be made dynamic for other devs to build project
# TODO set up environment variables file for multiple users

if not sys.platform.startswith('win'):
    project_path = '/Users/Jayme/OneDrive/Python/SMS' 
else:
    project_path = 'Y:/OneDrive/Python/SMS'

sys.path.append(project_path) # so we can import from guesttracker
from guesttracker import VERSION
from guesttracker import config as cf
from guesttracker import functions as f
from guesttracker import getlog
from guesttracker.utils import fileops as fl

log = getlog(__name__)

# check to make sure EL isn't running currently
procs = fl.find_procs_by_name('sms event log')
if procs:
    try:
        raise Exception('Close SMS Event Log.exe first!')
    finally:
        sys.exit(1)

datas = [
    ('guesttracker/_resources', '_resources'),
    ('SQL/FactoryCampaign/ac_motor_inspections.sql', '_resources/SQL')]

# WeasyPrint frozen True
# WeasyPrint _MEIPASS True
# WeasyPrint ROOT C:\Users\Jayme\Documents\guesttracker\dist\guesttracker_win\weasyprint
# cairocffi root:  C:\Users\Jayme\Documents\guesttracker\dist\guesttracker_win\cairocffi
# cairosvg frozen True
# cairosvg _MEIPASS True
# cairosvg ROOT C:\Users\Jayme\Documents\guesttracker\dist\guesttracker_win\cairosvg

# Don't need to modify __init__.py files anymore
package_imports = [
    # ['tinycss2', '', ('VERSION',)],  # tinycss doesnt use VERSION file anymore
    # ['cssselect2', '', ('VERSION',)],
    ['weasyprint', '', ('VERSION',)],
    ['cairosvg', '', ('VERSION',)],
    ['cairocffi', '', ('VERSION',)],
    ['pandas', 'io/formats/templates', ('',)],
    ['pyphen', 'dictionaries', ('',)],
    ['weasyprint', 'css', ('html5_ph.css', 'html5_ua.css')],
    ['plotly', 'package_data', ('',)],   
]

for package, subdir, files in package_imports:
    proot = os.path.dirname(importlib.import_module(package).__file__)
    datas.extend((os.path.join(proot, subdir, f), f'{package}/{subdir}') for f in files)

# PyInstaller searches functions too, not just module-level imports
hiddenimports = [
    'scipy.special.cython_special',
    # 'kaleido',
    'plotly.validators',
    'sentry_sdk.integrations.sqlalchemy',
    'sqlalchemy.sql.default_comparator'
    ]

# seems to work without plotly hidden modules
hidden_modules = [
    'guesttracker.queries']
    
for item in hidden_modules:
    hiddenimports.extend(collect_submodules(item))

log.info(f'Collected submodules: {f.deltasec(start)}')

# pandas just imports pyarrow I think (not using feather so hopefully not needed)
# babel comes from jinja2 as an optional extra
excludes = ['IPython', 'zmq', 'pyarrow', 'babel', 'sphinx', 'FixTk', 'tcl', 'tk', '_tkinter', 'tkinter', 'Tkinter', 'botocore', 'nbAgg', 'webAgg', 'Qt4Agg', 'Qt4Cairo']
sentry_excludes = [
    'sentry_sdk.integrations.aiohttp',
    'sentry_sdk.integrations.boto3',
    'sentry_sdk.integrations.django',
    'sentry_sdk.integrations.flask',
    'sentry_sdk.integrations.bottle',
    'sentry_sdk.integrations.falcon',
    'sentry_sdk.integrations.sanic',
    'sentry_sdk.integrations.celery',
    'sentry_sdk.integrations.rq',
    'sentry_sdk.integrations.tornado',
]
excludes.extend(sentry_excludes)
binaries = []

# seems to work on windows now?

if not cf.is_win:
    # binaries = [('/usr/local/bin/chromedriver', 'selenium/webdriver')]
    hookspath = [Path.home() / '.local/share/virtualenvs/SMS-4WPEHYhu/lib/python3.8/site-packages/pyupdater/hooks']
    dist_folder_name = 'guesttracker_mac'
    icon_name = 'sms_icon.icns'
    name_pyu = 'mac' # pyupdater needs to keep this name the same, is changed for final upload/dist

    run_pyupdater = 'pyupdater' in os.environ.get('_', '')
    py_ext = ('so',)

else:
    # binaries = [('File we want to copy', 'folder we want to put it in')]
    # binaries are analyzed for dependencies, datas are not
    # binaries = [('C:/Windows/chromedriver.exe', 'selenium/webdriver')]

    # gtk used for weasyprint/cairo to render images in pdfs. costs ~20mb to zip
    # NOTE maybe try to download this separately to avoid including in bundle
    # datas.append(('C:/Program Files/GTK3-Runtime Win64', 'GTK3-Runtime Win64'))  # maybe only need the binaries
    # binaries.append(('C:/Program Files/GTK3-Runtime Win64/bin', 'GTK3-Runtime Win64/bin'))

    # not sure if this is needed, seemed to work without
    hookspath = [Path.home() / 'AppData/Local/pypoetry/Cache/virtualenvs/guesttracker-vpjpMWts-py3.9/Lib/site-packages/pyupdater/hooks']
    dist_folder_name = 'guesttracker_win'
    icon_name = 'sms_icon.ico'
    name_pyu = 'win'
    run_pyupdater = os.environ.get('KMP_INIT_AT_FORK', None) is None # this key doesnt exist with pyu
    py_ext = ('pyd', 'dll')

upx = False
# upx_exclude = ['vcruntime140.dll', 'ucrtbase.dll', 'Qt5Core.dll', 'Qt5Core.dll', 'Qt5DBus.dll', 'Qt5Gui.dll', 'Qt5Network.dll', 'Qt5Qml.dll', 'Qt5QmlModels.dll', 'Qt5Quick.dll', 'Qt5Svg.dll', 'Qt5WebSockets.dll', 'Qt5Widgets.dll', 'Qt5WinExtras.dll', 'chromedriver.exe']
# if upx:
#     # exclude libs from upx
#     import pandas
#     import PyQt6
#     import scipy

#     for mod in (pandas, PyQt6, scipy):
#         p = Path(mod.__file__).parent
#         upx_exclude.extend([p.name for p in fl.find_files_ext(p, py_ext)])

icon = str(cf.p_res / f'images/{icon_name}')

if not eval(os.getenv('RUN_PYINSTALLER', 'False')):
    log.info('**** PYUPDATER ****')
    name = name_pyu # running from pyupdater
    dist_folder_name = name
    console = False
else:
    log.info('**** PYINSTALLER ****')
    name = 'SMS Event Log' # running from pyinstaller
    console = True

a = Analysis(
    [cf.p_root / 'run.py'],
    pathex=[cf.p_build],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=hookspath,
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False)

# pytz (pandas), tzdata (exchangelib) both add basically duplicates of all timezone info data files
# kaleido files downloaded separately
exclude_datas = ['kaleido', 'zoneinfo']
a.datas = TOC([x for x in a.datas if not any(item in x[0] for item in exclude_datas)])

# PyInstaller somehow analyzes appdata/local/../extensions libraries
exclude_binaries = ['GTK3-Runtime Win64']
a.binaries = TOC([x for x in a.binaries if not any(item in x[1] for item in exclude_binaries)])

pyz = PYZ(a.pure, a.zipped_data)

# TODO need to figure out which other files upx is messing up
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True, # False if using --onedir
    name=name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    # upx=upx,
    # upx_exclude=['vcruntime140.dll', 'ucrtbase.dll'],
    console=console,  # console=False means don't show cmd (only seems to work on windows)
    runtime_tmpdir=None,
    icon=icon,
    embed_manifest=False)

# using COLLECT means app will be '--onedir', cant use with '--onefile'
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    # upx=upx,
    # upx_exclude=upx_exclude,
    name=dist_folder_name,
    icon=icon,
    console=console)

if not cf.is_win:
    app = BUNDLE(
        coll,
        name=f'{name}.app',
        icon=icon,
        bundle_identifier='com.sms.guesttracker',
        info_plist={
            'NSPrincipalClass': 'NSApplication',
            'NSAppleScriptEnabled': 'YES',
            'NSAppleEventsUsageDescription': 'SMS Event Log would like to automate actions in other applications.',
            'CFBundleShortVersionString': VERSION,
            'CFBundleVersion': VERSION,
            },
        )

log.info(f'Finished: {f.deltasec(start)}')
