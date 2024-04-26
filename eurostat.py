import argparse
import tomllib

import pandasdmx as sdmx

def load_toml(filename):
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

def geo_series():
    estat = sdmx.Request('ESTAT')
    codelist = estat.codelist('GEO').codelist['GEO']
    series = sdmx.to_pandas(codelist).name
    return series

def nuts_one(country, level, series):
    country_filter = series.index.str.startswith(country)
    level_filter = series.index.str.len() == 2 + level
    current_filter = ~series.str.contains('NUTS', regex=False)
    extra_filter = ~series.str.contains('(', regex=False)
    all_filter = country_filter & level_filter & current_filter & extra_filter
    return series[all_filter]

def nuts_many(country_levels, series):
    result = []
    for country, level in country_levels:
        item = nuts_one(country, level, series)
        result.append(item)
    return result

def prepare_query(name, country, time, series):
    queries, meta = load_toml('eurostat.toml')
    query = queries[name]
    defaults = query['defaults']
    level = meta['NUTS'][country]
    country_levels = [(country, level)]
    nuts = nuts_many(country_levels, series)
    geo = []
    for nut in nuts:
        geo.extend(nut.index)
    start = time
    end = time
    key = dict(geo=geo, **defaults)
    params = dict(startPeriod=start, endPeriod=end)
    return key, params

def query(name, key, params):
    queries, meta = load_toml('eurostat.toml')
    query = queries[name]
    table = query['table']
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('query')
    parser.add_argument('country')
    parser.add_argument('time')
    args = parser.parse_args()
    print('Querying metadata')
    series = geo_series()
    key, params = prepare_query(args.query, args.country, args.time, series)
    print('Querying data')
    series = query(args.query, key, params)
    print(f'Retrieved {series.size} values')
    filename = f'{args.country}-{args.time}.csv'
    series.to_csv(filename)
    print(f'Wrote {filename}')
