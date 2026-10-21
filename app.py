import tempfile
import hashlib
from pathlib import Path

import pandas as pd
import streamlit as st

from main.common.file_classifier import FileClassifier
from main.common.logging_config import configure_logging
from main.ui.desktop_page import render_desktop
from main.ui.prep_page import render_prep


SUPPORTED_UPLOAD_TYPES = ['tfl', 'tflx', 'twb', 'twbx', 'tds', 'tdsx']
PREP_SUFFIXES = {'.tfl', '.tflx'}
DESKTOP_SUFFIXES = {'.twb', '.twbx', '.tds', '.tdsx'}


def process_upload(uploaded_file, logger):
  """Save one upload temporarily, classify it, and render its report."""
  file_name = uploaded_file.name
  file_data = uploaded_file.getvalue()
  # Keep the identifier stable across Streamlit reruns. UI interactions rerun the
  # script, and a random value here used to invalidate every conversion cache.
  run_id = hashlib.sha256(file_data).hexdigest()[:8]
  logger.info('[%s][%s] Processing started', run_id, file_name)

  with tempfile.TemporaryDirectory() as temporary_directory:
    path = Path(temporary_directory) / file_name
    path.write_bytes(file_data)
    loaded = FileClassifier(path, run_id).process()

    if loaded.suffix in PREP_SUFFIXES:
      render_prep(loaded.data_dict, file_name, run_id, logger)
    elif loaded.suffix in DESKTOP_SUFFIXES:
      render_desktop(loaded.data_xml, file_name, run_id, logger)

  logger.info('[%s][%s] Processing completed', run_id, file_name)


def main():
  st.set_page_config(page_title='Tableau File Converter', layout='wide')
  st.title('Tableau File Converter')

  debug = st.toggle('Debug mode')
  logger, log_stream = configure_logging(debug)
  if debug:
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

  uploaded_files = st.file_uploader(
    'Upload your Tableau file',
    type=SUPPORTED_UPLOAD_TYPES,
    accept_multiple_files=True,
  )
  for uploaded_file in uploaded_files or []:
    st.header(uploaded_file.name)
    try:
      process_upload(uploaded_file, logger)
    except Exception as exc:
      st.error(f'Could not process {uploaded_file.name}: {exc}')
      logger.exception('Error while processing %s', uploaded_file.name)

  if uploaded_files:
    st.download_button(
      label='Download app log',
      data=log_stream.getvalue(),
      file_name='app.log',
      mime='text/plain',
    )


if __name__ == '__main__':
  main()
