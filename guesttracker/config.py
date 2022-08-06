import os
import sys
from pathlib import Path
from typing import Any, Dict

import requests
import yaml

URL_REPO = 'https://raw.githubusercontent.com/jaymegordo/guesttracker/main'
URL_CONFIG = f'{URL_REPO}/guesttracker/_resources/config.yaml'

# Set environments
AZURE_LOCAL = not os.getenv('AZURE_FUNCTIONS_ENVIRONMENT') is None  # dont think these are used
AZURE_WEB = not os.getenv('WEBSITE_SITE_NAME') is None
AZURE = AZURE_LOCAL or AZURE_WEB
IS_QT_APP = not os.getenv('IS_QT_APP') is None
SYS_FROZEN = getattr(sys, 'frozen', False)

if sys.platform.startswith('win'):
    p_applocal = Path.home() / 'AppData/Local/SMS Equipment Inc/SMS Event Log'
    p_drive = Path('P:\\')
    platform = 'win'
    is_win = True
else:
    p_applocal = Path.home() / 'Library/Application Support/SMS Event Log'
    p_drive = Path('/Volumes/Public')
    platform = 'mac'
    is_win = False


p_temp = p_applocal / 'temp'
p_ext = p_applocal / 'extensions'
p_topfolder = Path(__file__).parent  # guesttracker
p_root = p_topfolder.parent  # SMS
p_build = Path.home() / 'Documents/guesttracker'
desktop = Path.home() / 'Desktop'
p_sql = p_root / 'SQL'


def add_to_path(p: Path) -> None:
    os.environ['PATH'] = os.pathsep.join([os.environ['PATH'], str(p)])


# GTK on win DOES need complete folder, not just /bin (fonts + renering .jpg)
p_gtk = p_ext / 'GTK3-Runtime Win64/bin'
p_kaleido = p_ext / 'kaleido/kaleido'

if is_win:
    # add manual gtk path to PATH for weasyprint/cairo if windows
    add_to_path(p_gtk)

# Add this to path so plotly can find Kaleido executable
# win is actually path to kaleido.cmd
add_to_path(p_kaleido)
kaleido_path = str(p_kaleido)

if SYS_FROZEN:
    p_topfolder = p_root

p_res = p_topfolder / '_resources'  # data folder location is shifted out of guesttracker for frozen build
p_sec = p_res / 'secret'
os.environ['p_secret'] = str(p_sec)
os.environ['p_unencrypt'] = str(p_topfolder / '_unencrypted')

p_config = p_res / 'config.yaml'

# set file log path for jgutils.logger
if not AZURE_WEB:
    p_log = p_applocal / 'logging/guesttracker.log'
    if not p_log.parent.exists():
        p_log.parent.mkdir(parents=True)

    os.environ['file_log_path'] = str(p_log)

# copy sql files to p_res when frozen
if SYS_FROZEN:
    p_sql = p_res / 'SQL'


def set_config() -> Dict[str, Any]:
    """Config is yaml file with config details, dictionary conversions etc"""
    with open(p_config, encoding='utf8') as file:
        return yaml.full_load(file)


def load_config_remote() -> Dict[str, Any]:
    """Load config from remote github repo on startup"""

    html = requests.get(URL_CONFIG).text
    m = yaml.full_load(html)

    # write to file
    with open(p_config, 'w', encoding='utf8') as file:
        yaml.dump(m, file)

    return m


def set_config_remote() -> None:
    """Load config from gh in worker thread"""
    if SYS_FROZEN:
        sys.modules[__name__].config = load_config_remote()


# load config on startup
config = set_config()
config_platform = config['Platform'][platform]
