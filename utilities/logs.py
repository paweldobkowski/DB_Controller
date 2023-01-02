import logging
import sys
import os


class Logs:
    def __init__(self, method):
        self.method = method
        self.counter = 0

    def __call__(self, *args, **kwargs):
        self.counter += 1

        return self.method(*args, **kwargs)

    def initialize_logs(path):
        # counting logs

        # if directory logs does not exists, create it:
        if not os.path.exists("Logs"):
            os.makedirs("Logs")

        logging.debug = Logs(logging.debug)
        logging.info = Logs(logging.info)
        logging.warning = Logs(logging.warning)
        logging.error = Logs(logging.error)
        logging.critical = Logs(logging.critical)

        logging.basicConfig(
            level=logging.INFO,  # lowest level printed
            format="%(asctime)s - [%(levelname)s] :: %(message)s",  # format of message
            handlers=[
                logging.FileHandler(path),  # print to file
                logging.StreamHandler(sys.stdout),
            ],  # print to console
        )

    def stop_logs(self):
        logging.info("### PODSUMOWANIE ###")
        logging.info(f"Wystapien warningow : {str(logging.warning.counter)}")
        logging.info(f"Wystapien errorow   : {str(logging.error.counter)}")
        logging.info(f"Wystapien criticali : {str(logging.critical.counter)}")
        logging.info("####################")
        logging.info("")
        logging.info("Koniec programu")
        logging.info("")
        logging.info(
            "Jezeli program wykonano poprawnie to Exit code = 0, jezeli wystapily jakiekolwiek bledy to Exit code = 1"
        )
        if (logging.warning.counter + logging.error.counter + logging.critical.counter) != 0:
            logging.info("Exit code = 1")
        else:
            logging.info("Exit code = 0")