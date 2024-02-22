import os
import random
import re
import time
import pandas as pd
import requests
import sqlite3
from urllib.parse import urljoin
import csv


cik_base_url = f"https://data.sec.gov/submissions/"  # url: https://data.sec.gov/submissions/CIK0001708269.json
sec_data_base_url = "https://www.sec.gov/Archives/"
target_forms = ["4", "S-1", "S-3", "10-Q", "10-K", "F-1", "8-K"]
idx_files_directory = os.path.join("resources", "idx_files")


def create_table():
    conn = sqlite3.connect("sec_data.db")
    c = conn.cursor()

    # #### c.execute("DROP TABLE IF EXISTS filings")
    c.execute(
        """CREATE TABLE IF NOT EXISTS filings
                (file_url TEXT, company_name TEXT, form_type TEXT, cik TEXT, date_filed TEXT)"""
    )
    # #### c.execute("DROP TABLE IF EXISTS ticker_sic")
    c.execute(
        """CREATE TABLE IF NOT EXISTS ticker_sic
                (cik TEXT PRIMARY KEY, tickers TEXT, sic TEXT)"""
    )
    conn.commit()
    conn.close()


def read_filings_table():
    conn = sqlite3.connect("sec_data.db")
    c = conn.cursor()
    c.execute("SELECT * FROM filings")
    data = {}
    columns = [desc[0] for desc in c.description]  # Get column names
    file_url_index = columns.index("file_url") if "file_url" in columns else None
    if file_url_index is not None:
        for row in c.fetchall():
            file_url = row[file_url_index]
            other_columns = [col for col in columns if col != "file_url"]
            data[file_url] = dict(
                zip(other_columns, row[:file_url_index] + row[file_url_index + 1 :])
            )
    else:
        print("File URL column not found in the result set.")
    conn.close()
    return data


def read_ticker_sic_table():
    conn = sqlite3.connect("sec_data.db")
    c = conn.cursor()
    c.execute("SELECT * FROM ticker_sic")
    data = {}
    columns = [desc[0] for desc in c.description]  # Get column names
    cik_index = columns.index("cik") if "cik" in columns else None
    if cik_index is not None:
        for row in c.fetchall():
            cik = row[cik_index]
            other_columns = [col for col in columns if col != "cik"]
            data[cik] = dict(zip(other_columns, row[:cik_index] + row[cik_index + 1 :]))
    else:
        print("CIK column not found in the result set.")
    conn.close()
    return data


def parse_idx_file_and_store(idx_file, filing_table_data, ticker_sic_table_data):
    records = []
    ticker_sic_mem = {}
    try:
        with open(idx_file, "r") as file:
            content = file.read()
    except Exception as ex:
        print(f"    Failed to read the file: {idx_file}. Exception: {ex}")
    else:
        start_idx = content.find("---")
        try:
            if start_idx != -1:
                content = content[start_idx:]
                lines = content.split("\n")

                conn = sqlite3.connect("sec_data.db")
                c = conn.cursor()

                for i, line in enumerate(lines[1:], start=1):
                    company_name = form_type = CIK = date_filed = file_name = ticker = (
                        sic
                    ) = None
                    print(f"    {i}/{len(lines[1:])}")
                    columns = re.split(r"\s{2,}", line.strip())
                    if len(columns) >= 5:
                        company_name = columns[0]
                        form_type = columns[1]
                        CIK = columns[2]
                        date_filed = columns[3]
                        file_name = columns[4]

                        file_link = urljoin(sec_data_base_url, file_name)
                        if (form_type in target_forms) and (
                            file_link not in filing_table_data
                        ):
                            c.execute(
                                "INSERT INTO filings VALUES (?, ?, ?, ?, ?)",
                                (file_link, company_name, form_type, CIK, date_filed),
                            )

                            temp = {
                                "name": company_name,
                                "form-type": form_type,
                                "cik": CIK,
                                "date-filed": date_filed,
                                "file-url": file_link,
                            }

                            # Fetch tickers and SIC and store in the ticker_sic table
                            if CIK in ticker_sic_table_data:
                                ticker_sic_data = ticker_sic_table_data[CIK]
                                print(
                                    f"      Got ticker and SIC from database | CIK: {CIK}"
                                )
                            elif CIK in ticker_sic_mem:
                                ticker_sic_data = ticker_sic_mem[CIK]
                                print(
                                    f"      Got ticker and SIC from memory | CIK: {CIK}"
                                )
                            else:
                                ticker_sic_data = get_ticker_sic(CIK)
                                time.sleep(random.uniform(0.2, 0.7))

                            if ticker_sic_data:
                                if type(ticker_sic_data["tickers"]) == type([]):
                                    ticker = ", ".join(
                                        ticker for ticker in ticker_sic_data["tickers"]
                                    )
                                else:
                                    ticker = ticker_sic_data["tickers"]
                                sic = ticker_sic_data["sic"]
                                c.execute(
                                    "INSERT OR REPLACE INTO ticker_sic VALUES (?, ?, ?)",
                                    (
                                        CIK,
                                        ticker,
                                        sic,
                                    ),
                                )

                                ticker_sic_mem[CIK] = {"tickers": ticker, "sic": sic}
                                temp["ticker"] = ticker
                                temp["sic"] = sic
                            records.append(temp)
                        elif not (form_type in target_forms):
                            print(f"      Skipping record | Form Type: {form_type}")
                        else:
                            if ((form_type in target_forms) and (file_link in filing_table_data)):
                                print(f"      Done Already")
                                # print(f"***************************")
                                # print(f"Done Already")
                                # print(f"***************************")
                conn.commit()
                conn.close()
        except Exception as ex:
            print(f" Error parsing the file: {idx_file} - Exception: {ex}")
            return None
        else:
            return records


def get_ticker_sic(cik):
    headers = {
        "authority": "efts.sec.gov",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.sec.gov",
        "referer": "https://www.sec.gov/",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    url = os.path.join(cik_base_url, f"CIK{cik.rjust(10, '0')}.json")
    print(f"      Requesting {url}")
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=60,
        )
        content = response.json()
    except Exception as ex:
        print(f"      Request Failed: url: {url} - Exception: {ex}")
    else:
        if response.status_code == 200:
            tickers = content.get("tickers", [])
            sic = content.get("sic", "")
        else:
            print(f"     Request Failed with status code {response.status_code}")
            return None

    return {"cik": cik, "tickers": tickers, "sic": sic}


def run_parser():
    create_table()  # Initialize the database and create tables

    data = []
    print("Processing idx Files...")
    for i, idx_file in enumerate(os.listdir(idx_files_directory[:25]), start=1):
        file_path = os.path.join(idx_files_directory, idx_file)
        print(f"  {i}/{len(os.listdir(idx_files_directory))} - {file_path}")
        filing_data = read_filings_table()
        ticker_sic_data = read_ticker_sic_table()
        parsed_data = parse_idx_file_and_store(file_path, filing_data, ticker_sic_data)
        if parsed_data is not None:
            data.extend(parsed_data)

    # master_data = []
    # print(f"Getting Tickers and SIC...")
    # for i, record in enumerate(data, start=1):
    #     print(f'  {i}/{len(data)} - {record["cik"]}')
    #     ticker_sic = get_ticker_sic(record["cik"])
    #     time.sleep(random.uniform(0.3, 0.8))
    #     temp_dict = {**record}
    #     temp_dict["ticker"] = ", ".join(ticker for ticker in ticker_sic["tickers"])
    #     temp_dict["sic"] = ticker_sic["sic"]
    #     master_data.append(temp_dict)

    output = pd.DataFrame(data)
    output.to_csv(
        "sec_data.csv",
        encoding="utf-8",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        index=False,
    )


if __name__ == "__main__":
    run_parser()
