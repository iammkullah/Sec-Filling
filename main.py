import time
from parse_idx_files import run_parser
from merge_tables import merge_tables_and_store
from download_idx_file import start_downloading

if __name__ == '__main__':
  try:
      print("----------------- Starting the downloading -------------------")
      start_downloading()   #one time process
      time.sleep(10)
      print("----------------- Processing the files -----------------------")
      run_parser()
      merge_tables_and_store()
      pass
  except Exception as ex:
      print(f"Error Scraping SEC - Exception: {ex}")
