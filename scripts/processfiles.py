"""
Command line script to import fault, haul, or fix dls folders
"""

import argparse

import guesttracker.data.internal.plm as plm
from guesttracker import delta, dt, getlog
from guesttracker.data.internal.utils import FileProcessor

log = getlog(__name__)

CLI = argparse.ArgumentParser()
CLI.add_argument(
    '--ftype',
    type=str,
    default=None)

# Units must be proper case eg F301 F302
# NOTE NO commas, only spaces!
CLI.add_argument(
    '--units',  # name on the CLI - drop the `--` for positional/required parameters
    nargs='*',  # 0 or more values expected => creates a list
    type=str,
    default=[],
    help='Leave blank to process all FH units')

CLI.add_argument(
    '--range',
    nargs='*',
    type=str,
    default=[])

CLI.add_argument(  # process units in 5 batches of 10
    '--batch',
    type=int,
    default=None)

CLI.add_argument(
    '--startdate',
    type=str,
    default=None)

CLI.add_argument(
    '--minesite',
    type=str,
    default=None)

CLI.add_argument(
    '--model',
    type=str,
    default=None)

if __name__ == '__main__':
    a = CLI.parse_args()
    units, ftype, rng, startdate, batch, = a.units, a.ftype, a.range, a.startdate, a.batch

    if startdate:
        d = dt.strptime(startdate, '%Y-%m-%d')
    else:
        d = dt.now() + delta(days=-31)

    if ftype == 'dsc':
        # process all units >>> prp scripts.processfiles --ftype dsc --startdate 2021-08-01
        FileProcessor(ftype=ftype, d_lower=d).process(units=units)
    elif ftype == 'plm':
        # TODO need to set up PLM with FileProcessor
        raise NotImplementedError('PLM not set up yet!')
        plm.update_plm_all_units(minesite=a.minesite, model=a.model)
    # else:
    #     log.info(f'ftype: {ftype}, units: {units}, startdate: {d}')
    #     utl.process_files(ftype=ftype, units=units, d_lower=d)
