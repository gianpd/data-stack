import os
import sys
import pathlib

from utils import logger

from datetime import datetime

import click

from dlcm import DLCM

dlcm = DLCM()

@click.group()
def cli():
    """
    Simple CLI for the Data-Ingest container.
    Use it for Async Task.
    """
    pass

@cli.command()
@click.option('--start-day', default=None)
@click.option('--end-day', default=None)
def apply_daily_lifecycle(start_day, end_day):
    """
    Run DAILY LIFECYCLE Action for Data-Ingest: create zip archive with the received json events.
    """
    log_prefix = 'apply_daily_lifecycle>'
    logger.info(f'{log_prefix} Applying zip event job ...')
    dlcm.apply_zip_events_job(start_day, end_day)

if __name__ == "__main__":
    cli()


    
