from bs4 import BeautifulSoup
import httpx
import asyncio
from functools import reduce
import json
from util.http_utils import get_url, download_file

ROOT_PAGE = "https://datashare.nida.nih.gov"


async def get_all_pages():
    response = await get_url(ROOT_PAGE + "/data")
    page = response.text
    soup = BeautifulSoup(page, features="html.parser")
    pager_li = soup.find_all("li", class_="pager__item")
    links = []
    for li in pager_li:
        a_tag = li.find('a')
        if a_tag:
            links.append(f"{ROOT_PAGE}/data{a_tag['href']}")
    return links


async def get_links_by_path(page_url, filter):
    resp = await get_url(url=page_url)
    page = resp.text
    soup = BeautifulSoup(page, features="html.parser")
    links = soup.find_all("a")
    result = []
    for link in links:
        if filter(link):
            result.append(f"{ROOT_PAGE}{link['href']}")
    return result


async def get_study_links_from_page(page_url):
    return await get_links_by_path(page_url, filter=lambda x: x.get('href', '').startswith("/study"))


async def get_study_page_as_json(page_url):
    page_as_json = {
        "study_url": page_url,
    }
    resp = await get_url(url=page_url)
    page = resp.text
    soup = BeautifulSoup(page, features="html.parser")
    # find general study info
    info_div = soup.find("div", class_="group-left")
    fields = info_div.find_all('div', class_="field")

    for field in fields:
        label = field.find("div", class_="field__label")
        if label:
            label = label.string.lower().strip(' ')
            item = field.find("div", class_="field__item")
            if item:
                item = item.text
                page_as_json[label] = item

    divs = soup.find_all("div", class_="field--name-field-clintri-protocol")
    links = []
    for div in divs:
        label = div.find("div", class_="field__label")
        is_protocol = label and label.string.lower() == "protocol"
        if is_protocol:
            print('found protocol')
            links.append(div.find('a').get('href'))
    if len(links):
        page_as_json['protocol_file'] = links[0]
    return page_as_json


async def get_all_study_links():
    pages = await get_all_pages()
    tasks = []
    for page in pages:
        tasks.append(get_study_links_from_page(page_url=page))
    study_pages = reduce(lambda a, b: a + b, await asyncio.tasks.gather(*tasks), [])
    return study_pages


async def get_all_studies_as_json(study_pages):
    # chunk pages into 10
    chunk_size = 20
    page_chunks = [study_pages[start: start + chunk_size] for start in range(0, len(study_pages), chunk_size)]
    study_json = []
    for study_pages in page_chunks:
        tasks = []
        for page in study_pages:
            tasks.append(get_study_page_as_json(page))
        study_json += await asyncio.tasks.gather(*tasks)
    return study_json


async def download_all_protocol_files(study_json, data_path="data/"):
    chunk_size = 20
    chunks = [study_json[start: start+chunk_size]
              for start in range(0, len(study_json), chunk_size)]
    for chunked in chunks:
        tasks = []
        for study_data in chunked:
            download_link = study_data['protocol_file']
            tasks.append(download_file(download_link, data_path=data_path))
        local_file_paths = await asyncio.tasks.gather(*tasks)
        for file_path , study_data in zip(local_file_paths, chunked):
            study_data['local_protocol_file'] = file_path
    final_study_json = reduce(lambda a, b: a + b, chunks, [])
    return final_study_json


def parse_pdf(path):
    from pyxpdf import Document, Page, Config
    with open(path, 'rb') as file:
        pdf = Document(file)
    print(pdf.info())
    table_of_contents = ""
    table_of_contants_started = False

    for index, page in enumerate(pdf):
        text = page.text()
        if text.lower().startswith("table of contents"):
            table_of_contants_started = True
        if table_of_contants_started:
            # check if its end of table of contants
            if "abbreviations" in text.lower().split('\n')[0]:
                break
        if table_of_contants_started:
            table_of_contents += text

    import re
    # match x [:] ........... [#]
    TOC_regex = r"[a-zA-Z0-9\s\:]*\:\s*\.+\s*\d*\s"
    matches = re.finditer(TOC_regex, table_of_contents, re.IGNORECASE)
    for m in matches:
        gp = m.group()
        if ".." in gp:
            print(gp)


async def main():
    study_pages = await get_all_study_links()
    print(f"found {len(study_pages)} studies")
    studies_as_json = await get_all_studies_as_json(study_pages=study_pages)
    studies_as_json = await download_all_protocol_files(studies_as_json)
    with open('studies.json', 'w') as stream:
        json.dump(studies_as_json, fp=stream, indent=2)
    return studies_as_json



if __name__ == '__main__':
    results = asyncio.run(main())
    print(results)
