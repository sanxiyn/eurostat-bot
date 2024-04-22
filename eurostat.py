import argparse
import tomllib

import pandasdmx as sdmx

# See https://en.wikipedia.org/wiki/Nomenclature_of_Territorial_Units_for_Statistics
COUNTRY_LEVEL = {
    'DE': 1,
    'FR': 1,
    'IT': 2,
}

def geo_series():
    estat = sdmx.Request('ESTAT')
    codelist = estat.codelist('GEO').codelist['GEO']
    series = sdmx.to_pandas(codelist).name
    return series

def nuts_one(country, level, series=None):
    if series is None:
        series = geo_series()
    country_filter = series.index.str.startswith(country)
    level_filter = series.index.str.len() == 2 + level
    current_filter = ~series.str.contains('NUTS', regex=False)
    extra_filter = ~series.str.contains('(', regex=False)
    all_filter = country_filter & level_filter & current_filter & extra_filter
    return series[all_filter]

def nuts_many(country_levels, series=None):
    if series is None:
        series = geo_series()
    result = []
    for country, level in country_levels:
        item = nuts_one(country, level, series)
        result.append(item)
    return result

def prepare_query(countries, time, age=None, sex=None, series=None):
    if age is None:
        age = 'TOTAL'
    if sex is None:
        sex = 'T'
    if series is None:
        series = geo_series()
    if not isinstance(countries, list):
        countries = [countries]
    if not isinstance(time, tuple):
        start = time
        end = time
    else:
        start, end = time
    if not isinstance(age, list):
        age = [age]
    if not isinstance(sex, list):
        sex = [sex]
    country_levels = []
    for country in countries:
        level = COUNTRY_LEVEL[country]
        country_levels.append((country, level))
    nuts = nuts_many(country_levels)
    geo = []
    for nut in nuts:
        geo.extend(nut.index)
    key = dict(age=age, geo=geo, sex=sex)
    params = dict(startPeriod=start, endPeriod=end)
    return key, params

def query(name, key, params):
    with open('eurostat.toml', 'rb') as toml:
        queries = tomllib.load(toml)
    query = queries[name]
    table = query['table']
    dtype = query['dtype']
    levels = query['levels']
    drop_levels = query['drop_levels']
    drop_levels_if_singular = query['drop_levels_if_singular']
    estat = sdmx.Request('ESTAT')
    data = estat.data(table, key=key, params=params)
    series = sdmx.to_pandas(data)
    series = series.astype(dtype)
    series = series.rename(name)
    series = series.rename_axis(index=dict(geo='nuts', TIME_PERIOD='time'))
    series = series.reorder_levels(levels)
    index = series.index
    for name, level in zip(index.names, index.levels):
        if name in drop_levels_if_singular and len(level) == 1:
            drop_levels.append(name)
    series = series.droplevel(drop_levels)
    return series

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('query')
    parser.add_argument('country')
    parser.add_argument('time')
    args = parser.parse_args()
    print('Querying metadata')
    key, params = prepare_query(args.country, args.time)
    print('Querying data')
    series = query(args.query, key, params)
    print(f'Retrieved {series.size} values')
    filename = f'{args.country}-{args.time}.csv'
    series.to_csv(filename)
    print(f'Wrote {filename}')
