import os
import sys
import warnings

warnings.filterwarnings('ignore', '(?s).*MATPLOTLIBDATA.*', category=UserWarning)

if __name__ == '__main__':
    os.environ['IS_QT_APP'] = 'True'  # set env variable for qt app

    if True:
        import numpy as np  # noqa try to avoid "numpy has no attribute _CopyMode"

    # default is cairo, don't want to download libs yet
    import matplotlib
    matplotlib.use('AGG')

    # TODO delete this after a release or two
    from guesttracker import config as cf
    if cf.is_win and cf.SYS_FROZEN:
        p = cf.p_root / 'pyarrow'
        if p.exists():
            import shutil
            shutil.rmtree(p)

    from guesttracker.gui import startup
    sys.exit(startup.launch())
