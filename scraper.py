from pandas.tseries.offsets import MonthEnd
from urllib.parse import urljoin
from typing import Any
import pandas as pd
import numpy as np
import yaml
import requests
import logging

logger = logging.getLogger('promqueen')


class Scraper:
    def __init__(self, config: str, output: str):
        """Scrape the most accurate data from a given period.

        Scrape the last value of the given period. If empty, assess the previous
        value, and go on until a value is found that is not missing until the
        start of the period.
        """
        with open(config, 'r') as stream:
            self.config = yaml.safe_load(stream)

        self.output = output
        self.address = self.get('address')
        self.query = self.get('query')
        self.tz = self.get('timezone', 'CET')
        self.lookback = self.get('lookback', 3)  # look back N months
        self.step = self.get('step', '1m')
        self.endpoint = urljoin(self.address, '/api/v1/query_range')

        # Load existing data
        try:
            self.df = pd.read_csv(
                self.output, sep='\t',
                parse_dates=['utc_timestamp', 'local_timestamp']
            )
            self.df['period'] = pd.to_datetime(
                self.df.utc_timestamp,
                utc=True
            ).dt.tz_convert(self.tz).dt.to_period('M')

        except (FileNotFoundError, pd.errors.EmptyDataError):
            logger.info('Found no existing data at %s' % self.output)
            self.df = pd.DataFrame()

    def get(self, field, default: Any = None):
        """Read and marginally validate config file parameters"""
        value = self.config.get(field, default)
        if not default and not value:  # required field
            raise ValueError('%s is a required field' % field)
        else:
            return value

    def to_df(self, results: list) -> pd.DataFrame:
        """Convert Prometheus API result records to pandas DataFrame"""
        df = pd.DataFrame()
        for result in results:
            data = pd.DataFrame.from_records(
                result['values'],
                columns=['unix_time', 'value']
            )
            data['pdu'] = '.'.join(
                result['metric']['instance'].split('.', 2)[:2])
            df = pd.concat([df, data])

        # Format data and pivot table to have PDUs w/ values as columns
        df.value = df.value.astype(np.float64).round(2)
        df = df.pivot(index='unix_time', columns='pdu', values='value')
        df['utc_timestamp'] = pd.to_datetime(df.index, unit='s', utc=True)
        df['local_timestamp'] = df.utc_timestamp.dt.tz_convert(self.tz)
        df.dropna(inplace=True)

        if df.empty:
            logger.warning('No valid records returned')
            return pd.DataFrame()

        # Obtain most recent data and format for merging with output
        df.sort_index(ascending=True, inplace=True)
        df = df.iloc[[-1]]
        df['total'] = df.drop('local_timestamp', axis=1).sum(axis=1).round(2)
        df['period'] = pd.to_datetime(
            df.utc_timestamp,
            utc=True
        ).dt.tz_convert(self.tz).dt.to_period('M')
        df.reset_index(inplace=True, drop=True)

        return df

    def query_range(self, start, end) -> pd.DataFrame:
        """Request data from Prometheus"""
        start = start.astimezone('UTC').isoformat()
        end = end.astimezone('UTC').isoformat()
        params = {
            'query': self.query,
            'start': start,
            'end': end,
            'step': self.step
        }
        response = requests.get(self.endpoint, params=params).json()
        result = response.get('data', {}).get('result', [])
        status = response.get('status', '')
        error = response.get('error', '')
        logger.info('Querying data from %s until %s' % (start, end))

        if status == 'success' and result:
            return self.to_df(result)
        elif status == 'success':
            logger.debug('Query succeeded but no data points were returned')
        else:
            logger.warning('Query failed with %s response status: %s'
                           % (status, error))

        return pd.DataFrame()

    def add_row(self, data: pd.DataFrame):
        """If previous data exists, perform a sanity check. Data closer to the
        end of the month more accurately reflects usage of that period"""

        period = data.period[0]
        data.reset_index(inplace=True, drop=True)

        if self.df.empty:
            row = pd.DataFrame()
        else:
            row = self.df[self.df.period == period].reset_index(drop=True)

        if row.empty:
            logger.debug('[%s] Add new row' % period)
            self.df = pd.concat([self.df, data])
            return

        elif len(row) > 1:
            logger.error('[%s] %s rows found, 1 expected' % (period, len(row)))
            logger.info('[%s] Replace with scraped data' % period)
            self.df = pd.concat([self.df[self.df.period != period], data])
            return

        # Check if current data is more up to date
        if data.utc_timestamp[0] > row.utc_timestamp[0]:
            logger.info('[%s] Update to new data' % period)
            self.df = pd.concat([self.df[self.df.period != period], data])
            return

        elif data.utc_timestamp[0] == row.utc_timestamp[0]:
            drop = ['period', 'utc_timestamp', 'local_timestamp']
            if row.drop(columns=drop).equals(data.drop(columns=drop)):
                logger.info('[%s] Scraped data matches stored data' % period)
                return
            else:
                logger.info('[%s] Update data entry' % period)
                self.df = pd.concat([self.df[self.df.period != period], data])
                return

        else:
            logger.debug('[%s] Discard scraped data' % period)
            return

    def run(self):
        """Scrape data for each period from the current up until the
        look back period."""

        today = pd.to_datetime('today').tz_localize(self.tz)
        for i in range(0, self.lookback):
            data = pd.DataFrame()
            offset = pd.DateOffset(months=i)
            dt = today - offset + MonthEnd()
            dt = today if dt > today else dt
            period = pd.Period(dt, 'D')
            start = period.to_timestamp(how='start').tz_localize(self.tz)
            end = period.to_timestamp(how='end').tz_localize(self.tz)

            for j in range(0, end.day):
                data = self.query_range(
                    start - pd.DateOffset(days=j),
                    end - pd.DateOffset(days=j)
                )

                if not data.empty:
                    break

            if data.empty:
                logger.warning('No data returned for range [%s, %s]'
                               % (start, end))
                continue

            self.add_row(data)

        logger.debug('Storing changes to %s' % self.output)
        self.df.sort_values(by='utc_timestamp', ascending=True, inplace=True)
        self.df.drop(['period'], axis=1, inplace=True)

        # Reorder dataframe and format column names before saving
        self.df = self.df[
            ['utc_timestamp', 'local_timestamp'] +
            [c for c in self.df.columns
             if c not in ('utc_timestamp', 'local_timestamp', 'total')] +
            ['total']
        ]

        # Save output
        self.df.to_csv(self.output, sep='\t', index=False)
