import argparse
import logging
import tomllib
import warnings
from typing import Any

warnings.filterwarnings('ignore', module='pandasdmx')

import pandasdmx as sdmx  # type: ignore
from pandas import DataFrame, Series

logging.getLogger('pandasdmx.reader.sdmxml').setLevel(logging.CRITICAL)

StrDict = dict[str, Any]

def load_toml(filename: str) -> tuple[StrDict, StrDict]:
    with open(filename, 'rb') as toml:
        info = tomllib.load(toml)
    queries = {}
    meta = {}
    for key in info:
        if key.islower():
            queries[key] = info[key]
            queries[key]['name'] = key
        elif key.isupper():
            meta[key] = info[key]
    return queries, meta

def geo_series() -> Series:
    estat = sdmx.Request('ESTAT')
    codelist = estat.codelist('GEO').codelist['GEO']
    series = sdmx.to_pandas(codelist).name
    return series

def nuts_one(country: str, level: int, series: Series) -> Series:
    country_filter = series.index.str.startswith(country)
    level_filter = series.index.str.len() == 2 + level
    current_filter = ~series.str.contains('NUTS', regex=False)
    extra_filter = ~series.str.contains('(', regex=False)
    all_filter = country_filter & level_filter & current_filter & extra_filter
    return series[all_filter]

def nuts_many(country_levels: list[tuple[str, int]], series: Series) -> list[Series]:
    result = []
    for country, level in country_levels:
        item = nuts_one(country, level, series)
        result.append(item)
    return result

def prepare_query(query_info: StrDict, meta: StrDict, country: str, time: str, series: Series) -> tuple[StrDict, StrDict]:
    defaults = query_info['defaults']
    level = meta['NUTS'][country]
    country_levels = [(country, level)]
    nuts = nuts_many(country_levels, series)
    geo: list[str] = []
    for nut in nuts:
        geo.extend(nut.index)
    start = time
    end = time
    key = dict(geo=geo, **defaults)
    params = dict(startPeriod=start, endPeriod=end)
    return key, params

def execute_query(query_info: StrDict, key: StrDict, params: StrDict) -> Series:
    name = query_info['name']
    table = query_info['table']
    dtype = 'int64'
    levels = ['nuts', 'time']
    estat = sdmx.Request('ESTAT')
    data = estat.data(table, key=key, params=params)
    series = sdmx.to_pandas(data)
    series = series.rename(name)
    series = series.astype(dtype)
    series = series.rename_axis(index=dict(geo='nuts', TIME_PERIOD='time'))
    drop_levels = []
    for name in series.index.names:
        if name not in levels:
            drop_levels.append(name)
    series = series.droplevel(drop_levels)
    series = series.reorder_levels(levels)
    return series

def query_one(query_info: StrDict, meta: StrDict, country: str, time: str, geo_series: Series) -> Series:
    name = query_info['name']
    table = query_info['table']
    key, params = prepare_query(query_info, meta, country, time, geo_series)
    print(f'Querying {name} (table: {table})')
    series = execute_query(query_info, key, params)
    return series

def query_many(country: str, time: str) -> DataFrame:
    queries, meta = load_toml('eurostat.toml')
    print('Querying metadata')
    geo = geo_series()
    stats = {}
    for query in sorted(queries):
        query_info = queries[query]
        series = query_one(query_info, meta, country, time, geo)
        stats[query] = series
    dataframe = DataFrame(stats)
    return dataframe

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('country')
    parser.add_argument('time')
    args = parser.parse_args()
    dataframe = query_many(args.country, args.time)
    filename = f'{args.country}-{args.time}.csv'
    dataframe.to_csv(filename)
    print(f'Wrote {filename}')
