# promqueen
Python-based scraper for retrieving power usage from a
[prometheus.io](https://prometheus.io/)-tracked counter variable from the
Raritan PDU exporter.

This scraper uses the prometheus API to retrieve data from the
`raritan_pdu_activeenergy_watthour_total` metric. The data is
stored on a monthly basis, where each entry contains the last available
value of the metric for the associated month. Time is recorded both in CET
(local time) and UTC (universal time).

The default metric, 
`raritan_pdu_activeenergy_watthour_total{inletid=~".+", poleline=""}`,
collects **active energy** with the unit being **Watt hour (Wh)**. The
value is a counter and therefore only increases between months. To get the
monthly usage, subtract the previous month from the month you wish to obtain
the usage from.

## Install promqueen
promqueen can be installed using pip

```commandline
python3 -m pip install -r requirements.txt
```

promqueen can also be installed using apt

```commandline
xargs apt-get install <requirements.system
```


## Configure promqueen
Changes to the configuration of promqueen can be made by editing the
`config.yaml` file, which contains the following default values:

```
address: http://mon1.wct.inm7.de:9090
output: output/power_usage.tsv
timezone: CET
query: raritan_pdu_activeenergy_watthour_total{inletid=~".+", poleline=""}
step: 1m
lookback: 3
```

* The `address` entry should be an `http` link to the Prometheus Time-Series
  Collection and Processing server.
* The `output` entry is a file path to the output file, in which the data will
  be stored.
* The `timezone` entry is to inform promqueen about the local timezone.
* The `query` entry can be changed to a different metric, but this metric must
  be a counter to be interpret correctly by promqueen.
* The `step` entry determines the step size of the collected data when a
  Prometheus range-query is made. A step of `1m` results in a data point from
  the selected time range at each minute mark.
* The `lookback` entry tells promqueen how far to look back for missing data.
  This value is in months.

## Run promqueen
One promqueen scrape will be performed every time `corsage.sh` is executed.
This will also conveniently push the output file to the associated git
repository. It is recommended to execute promqueen once per day through an
automated process. This allows promqueen to stay up to date with the current
month's data.

```commandline
export PQ_OUTPUT_REMOTE=<remote-https-path-with-token>
./corsage.sh
```
