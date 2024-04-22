import argparse
import concurrent.futures
import re

import pywikibot
import pywikibot.site

COUNTRY_TEMPLATE = {
    'DE': '독일의 주',
    'FR': '프랑스의 레지옹',
}

INDENT_SIZE = 2

LINK_PATTERN = re.compile(r'\[\[([^\]]+)\]\]')

def replace_with_page(match):
    link = match.group(1)
    if '|' in link:
        page, text = link.split('|', 1)
        return page
    return link

def replace_with_text(match):
    link = match.group(1)
    if '|' in link:
        page, text = link.split('|', 1)
        return text
    return link

def replace_link_with_page(text):
    return LINK_PATTERN.sub(replace_with_page, text)

def replace_link_with_text(text):
    return LINK_PATTERN.sub(replace_with_text, text)

def print_indented(level, line):
    count = INDENT_SIZE * level
    print(' ' * count + line)

def get_navbox_pages(text):
    inside_navbox = False
    groups = []
    current_list = []
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

def get_template_page(site, title):
    template_ns = pywikibot.site.Namespace.TEMPLATE
    prefix = site.namespaces[template_ns].custom_prefix()
    page = pywikibot.Page(site, prefix + title)
    return page

def query_wikibase(ko_title):
    wikipedia_ko = pywikibot.Site('wikipedia:ko')
    wikipedia_en = pywikibot.Site('wikipedia:en')
    page = pywikibot.Page(wikipedia_ko, ko_title)
    item_page = page.data_item()
    qid = item_page.getID()
    en_title = item_page.getSitelink(wikipedia_en)
    return ko_title, qid, en_title

def print_hierarchy(country):
    wikipedia_ko = pywikibot.Site('wikipedia:ko')
    template_title = COUNTRY_TEMPLATE[country]
    print_indented(0, template_title)
    template_page = get_template_page(wikipedia_ko, template_title)
    navbox = get_navbox_pages(template_page.text)
    for group, pages in navbox:
        print_indented(1, group)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for result in executor.map(query_wikibase, pages):
                ko_title, qid, en_title = result
                print_indented(2, ko_title)
                print_indented(3, qid)
                print_indented(3, en_title)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('country')
    args = parser.parse_args()
    print_hierarchy(args.country)
