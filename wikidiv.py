import argparse
import concurrent.futures
import csv
import os.path
import re
import tomllib

import pywikibot
from pywikibot.page import BasePage
from pywikibot.site import BaseSite

CSV_OUTPUT = 'wikibase.csv'

INDENT_SIZE = 2

LINK_PATTERN = re.compile(r'\[\[([^\]]+)\]\]')

def replace_with_page(match: re.Match) -> str:
    link = match.group(1)
    if '|' in link:
        page, text = link.split('|', 1)
        return page
    return link

def replace_with_text(match: re.Match) -> str:
    link = match.group(1)
    if '|' in link:
        page, text = link.split('|', 1)
        return text
    return link

def replace_link_with_page(text: str) -> str:
    return LINK_PATTERN.sub(replace_with_page, text)

def replace_link_with_text(text: str) -> str:
    return LINK_PATTERN.sub(replace_with_text, text)

def print_indented(level: int, line: str) -> None:
    count = INDENT_SIZE * level
    print(' ' * count + line)

def get_navbox_pages(text: str) -> list[tuple[str, list[str]]]:
    inside_navbox = False
    groups = []
    current_list: list[str] = []
    lines = text.splitlines()
    for line in lines:
        if line.startswith('{{둘러보기 상자'):
            inside_navbox = True
            continue
        if line.startswith('}}'):
            inside_navbox = False
            continue
        if not inside_navbox:
            continue
        if line.startswith('|'):
            line = line.removeprefix('|')
            key, value = line.split('=', 1)
            key = key.strip()
            if key.startswith('묶음'):
                value = value.strip()
                group = replace_link_with_text(value)
                current_list = []
                groups.append((group, current_list))
            continue
        if line.startswith('*'):
            line = line.removeprefix('*')
            line = line.strip()
            page = replace_link_with_page(line)
            current_list.append(page)
    return groups

def get_template_page(site: BaseSite, title: str) -> BasePage:
    template_ns = pywikibot.site.Namespace.TEMPLATE  # type: ignore[attr-defined]
    prefix = site.namespaces[template_ns].custom_prefix()
    page = pywikibot.Page(site, prefix + title)
    return page

def query_wikibase(ko_title: str) -> tuple[str, str, str]:
    wikipedia_ko = pywikibot.Site('wikipedia:ko')
    wikipedia_en = pywikibot.Site('wikipedia:en')
    page = pywikibot.Page(wikipedia_ko, ko_title)
    item_page = page.data_item()
    qid = item_page.getID()
    en_title = item_page.getSitelink(wikipedia_en)
    return qid, ko_title, en_title

def print_one(country_info: dict[str, str]) -> None:
    wikipedia_ko = pywikibot.Site('wikipedia:ko')
    navbox_title = country_info['navbox_title']
    navbox_group = country_info['navbox_group']
    print_indented(0, navbox_title)
    navbox_page = get_template_page(wikipedia_ko, navbox_title)
    navbox = get_navbox_pages(navbox_page.text)
    exists = os.path.exists(CSV_OUTPUT)
    csvfile = open(CSV_OUTPUT, 'a')
    csvwriter = csv.writer(csvfile)
    fieldnames = ['qid', 'wikipedia:ko', 'wikipedia:en']
    if not exists:
        csvwriter.writerow(fieldnames)
    for group, pages in navbox:
        if group != navbox_group:
            continue
        print_indented(1, group)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for row in executor.map(query_wikibase, pages):
                qid, ko_title, en_title = row
                print_indented(2, ko_title)
                csvwriter.writerow(row)
    csvfile.close()

def print_many(lang: str) -> None:
    filename = f'{lang}.toml'
    with open(filename, 'rb') as toml:
        countries = tomllib.load(toml)
    for country in sorted(countries):
        country_info = countries[country]
        print_one(country_info)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('lang')
    args = parser.parse_args()
    print_many(args.lang)
