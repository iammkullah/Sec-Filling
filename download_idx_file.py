import os
import random
import time
from urllib.parse import urljoin
from datetime import datetime

import requests
from bs4 import BeautifulSoup


base_url = "https://www.sec.gov/Archives/edgar/daily-index/"
idx_files_directory = os.path.join("resources", "idx_files")
date_time = datetime.utcnow()


def get_year_list():
    year_links = []
    headers = {
        "authority": "www.sec.gov",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "if-modified-since": "0",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    try:
        response = requests.get(
            "https://www.sec.gov/Archives/edgar/daily-index/",
            headers=headers,
            timeout=60,
        )

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", attrs={"summary": "heding"})
            tds = table.find_all("td")

            for td in tds:
                a_tag = td.find("a", href=True)
                if a_tag is not None:
                    year_links.append(urljoin(base_url, a_tag["href"]))
                    if a_tag.text == f"{date_time.year}":
                        break
        else:
            print(
                f"    Failed to fetch year list - Status Code: {response.status_code}"
            )
    except Exception as ex:
        print(f"    Failed to fetch year list - Exception: {ex}")
        return None

    return year_links


def get_yearly_quarter_list(link):
    quarter_links = []
    headers = {
        "Referer": "https://www.sec.gov/Archives/edgar/daily-index/",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }

    try:
        response = requests.get(link, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", attrs={"summary": "heding"})
            tds = table.find_all("td")

            for td in tds:
                a_tag = td.find("a", href=True)
                if a_tag is not None:
                    if a_tag.text.startswith("QT"):
                        quarter_links.append(urljoin(link, a_tag["href"]))
        else:
            print(
                f"    Failed to fetch quarter list - Status Code: {response.status_code}"
            )
    except Exception as ex:
        print(f"    Failed to fetch quarter list - Exception: {ex}")
        return None

    return quarter_links


def get_idx_file_path(link):
    idx_files_path = []
    headers = {
        "Referer": "https://www.sec.gov/Archives/edgar/daily-index/1994/",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }

    try:
        response = requests.get(link, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", attrs={"summary": "heding"})
            tds = table.find_all("td")

            for td in tds:
                a_tag = td.find("a", href=True)
                if a_tag is not None:
                    if (a_tag.text.startswith("company") and a_tag.text.endswith(".idx")):
                        idx_files_path.append(urljoin(link, a_tag["href"]))
        else:
            print(
                f"    Failed to fetch idx files path - Status Code: {response.status_code}"
            )
    except Exception as ex:
        print(f"    Failed to fetch idx files path - Exception: {ex}")
        return None

    return idx_files_path


def make_download_dir(dir_name):
    try:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"Created directory successfully: {dir_name}")
        else:
            print(f"Directory {dir_name} already exists")
    except Exception as ex:
        print(f"Failed to create directory: {dir_name} - Exception: {ex}")
        return False
    else:
        return True


def download_idx_file(link, output_file_name):
    headers = {
        "authority": "www.sec.gov",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://www.sec.gov/Archives/edgar/daily-index/2024/QTR1/",
        "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }
    try:
        response = requests.get(link, headers=headers)

        if response.status_code == 200:
            content = response.text

            with open(output_file_name, "w") as file:
                file.write(content)
        else:
            print(
                f"    Failed to download the file. Status code: {response.status_code}"
            )
    except Exception as ex:
        print(f"    Failed to download the file. - Exception: {ex}")


def start_downloading():
    quarter_list = []
    year_list = get_year_list()
    print(f"Fetching yearly Quarter list...")
    for i, url in enumerate(year_list, start=1):
        print(f"  {i}/{len(year_list)} - {url}")
        res_content = get_yearly_quarter_list(url)
        if res_content is not None:
            quarter_list.extend(res_content)
        time.sleep(random.uniform(0.3, 0.8))
    print()

    idx_files = []
    print(f"Fetching idx files url...")
    for i, quarter_link in enumerate(quarter_list, start=1):
        print(f"  {i}/{len(quarter_list)} - {quarter_link}")
        res_content = get_idx_file_path(quarter_link)
        if res_content is not None:
            idx_files.extend(res_content)
        time.sleep(random.uniform(0.3, 0.8))
    print()

    print(f"Creating the downloads directory: {idx_files_directory}...")
    dir_created = make_download_dir(idx_files_directory)
    if dir_created:
        already_downloaded_files = [os.path.join(idx_files_directory, file) for file in os.listdir(idx_files_directory)]
        print(f"downloading idx files...")
        for i, idx_link in enumerate(idx_files, start=1):
            print(f"  {i}/{len(idx_files)} - {idx_link}")
            file_name = idx_link.split(r"/")[-1]
            output_file_name = os.path.join(idx_files_directory, file_name)
            if output_file_name in already_downloaded_files:
                print(f"    File {output_file_name} already downloaded")
                continue
            download_idx_file(idx_link, output_file_name)
            time.sleep(random.uniform(0.3, 0.8))
    
if __name__ == "__main__":
    start_downloading()