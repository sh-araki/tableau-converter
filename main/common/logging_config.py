import io
import logging


HANDLER_MARKER = '_tableau_converter_handler'
LOG_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s'


def configure_logging(debug=False):
  """Configure one in-memory application log handler per Streamlit run."""
  log_stream = io.StringIO()
  root_logger = logging.getLogger()

  for existing_handler in list(root_logger.handlers):
    if getattr(existing_handler, HANDLER_MARKER, False):
      root_logger.removeHandler(existing_handler)

  handler = logging.StreamHandler(log_stream)
  handler.setFormatter(logging.Formatter(LOG_FORMAT))
  setattr(handler, HANDLER_MARKER, True)
  root_logger.addHandler(handler)
  root_logger.setLevel(logging.DEBUG if debug else logging.INFO)
  return logging.getLogger('tableau_converter'), log_stream

