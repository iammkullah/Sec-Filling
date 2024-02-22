import sqlite3


def merge_tables_and_store():
    conn = sqlite3.connect("sec_data.db")
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS merged_data")

    c.execute(
        """
        CREATE TABLE merged_data (
            file_url TEXT,
            company_name TEXT,
            form_type TEXT,
            cik TEXT,
            date_filed TEXT,
            tickers TEXT,
            sic TEXT
        )
        """
    )
    c.execute(
        """
        INSERT INTO merged_data
        SELECT filings.file_url, filings.company_name, filings.form_type, filings.cik, filings.date_filed, ticker_sic.tickers, ticker_sic.sic
        FROM filings
        JOIN ticker_sic ON filings.cik = ticker_sic.cik
    """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    merge_tables_and_store()
