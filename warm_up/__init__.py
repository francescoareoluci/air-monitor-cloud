import logging
import datetime
import azure.functions as func

## Main function
# A simple function to keep warm the other Azure functions
def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer function fired.')

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('Warm up done at %s', utc_timestamp)