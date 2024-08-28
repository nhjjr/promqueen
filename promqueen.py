#!/usr/bin/env python3
from scraper import Scraper

import argparse
import logging
import warnings

"""
$ python3 promqueen.py -c config.yaml -o output.tsv
"""

# Disable warnings
warnings.filterwarnings("ignore")

# External (root level) logging level
logging.basicConfig(level=logging.ERROR)

# Internal logging level
logger = logging.getLogger('promqueen')
logger.setLevel(level=logging.ERROR)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Python-based Prometheus collector for data retention'
    )
    parser.add_argument(
        '-c', '--config',
        metavar='config',
        required=False,
        default='config.yaml',
        help='promqueen configuration file'
    )
    parser.add_argument(
        '-o', '--output',
        metavar='output',
        required=True,
        help='promqueen output file'
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = 'config.yaml' if not args.config else args.config
    scraper = Scraper(config, args.output)
    scraper.run()


if __name__ == '__main__':
    main()
