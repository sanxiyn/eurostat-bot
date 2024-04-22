import pandas
import pywikibot

from colorama import init
from colorama import Back, Fore, Style

INDNT_SIZE = 2

def print_indented(level, line):
    count = INDNT_SIZE * level
    print(' ' * count + line)

def get_infobox(lines):
    inside_infobox = False
    result = []
    for i, line in enumerate(lines):
        if line.startswith('{{독일 주 정보'):
            inside_infobox = True
            continue
        if line.startswith('}}'):
            inside_infobox = False
            continue
        if not inside_infobox:
            continue
        if line.startswith('|'):
            line = line.removeprefix('|')
            key, value = line.split('=', 1)
            key = key.strip()
            if key.startswith('인구'):
                value = value.strip()
                result.append((i, key, value))
    return result

def get_page_text(site, title):
    page = pywikibot.Page(site, title)
    return page.text

def set_page_text(site, title, text):
    page = pywikibot.Page(site, title)
    page.text = text
    page.save()

mapping = pandas.read_csv('mapping.csv', index_col='iso')
eurostat = pandas.read_csv('DE-2023.csv', index_col='nuts')
wikibase = pandas.read_csv('wikibase.csv', index_col='qid')

ko = pywikibot.Site('wikipedia:ko')
en = pywikibot.Site('wikipedia:en')

init()

country = 'DE'
print_indented(0, country)
for iso in mapping.index:
    if not iso.startswith(country):
        continue
    row = mapping.loc[iso]
    nuts = row['nuts']
    qid = row['qid']
    eurostat_row = eurostat.loc[nuts]
    time = eurostat_row['time']
    population = eurostat_row['population']
    wikibase_row = wikibase.loc[qid]
    ko_title = wikibase_row[ko.sitename]
    en_title = wikibase_row[en.sitename]
    text = get_page_text(ko, ko_title)
    lines = text.splitlines()
    infobox = get_infobox(lines)
    update = False
    for i, key, value in infobox:
        if key == '인구_날짜':
            old_dates = ['2006', '2012']
            for old_date in old_dates:
                if old_date in value:
                    update = True
    if not update:
        continue
    print_indented(1, f'{iso} {nuts} {qid} {ko_title} {en_title}')
    for i, key, value in infobox:
        print(Fore.RED + '-' + lines[i])
    for i, key, value in infobox:
        if key == '인구':
            line = f'| 인구 = {population}'
        if key == '인구_날짜':
            line = f'| 인구_날짜 = {time}-01-01'
            line += '<ref>{{웹 인용 |url=https://doi.org/10.2908/DEMO_R_D2JAN |제목=Population on 1 January by age, sex and NUTS 2 region |웹사이트=[[유럽 연합 통계국]] }}</ref>'
        print(Fore.GREEN + '+' + line)
        lines[i] = line
    print(Style.RESET_ALL, end='')
    input()
    text = '\n'.join(lines)
    set_page_text(ko, ko_title, text)
