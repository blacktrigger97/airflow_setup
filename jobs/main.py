import logging
import logging.config
import sys
from pprint import pformat
from time import sleep
from typing import Any
from datetime import datetime
from pathlib import Path
from multiprocessing_logging import install_mp_handler
from pystrm.utils.mainCalls.yfUtils import  getLiveTickData
from pystrm.utils.confs import fetch_conf, fetch_prop
from pystrm.utils.constants import RUN_ID
from pystrm.utils.logger.logDecor import inOutLog


@inOutLog
def main() -> None: 

    Path(Path.cwd()/'logs').mkdir(exist_ok=True)

    config: dict[str, Any] = fetch_conf()['Logging']
    config['handlers']['file_json']['filename'] += datetime.now().strftime('%Y-%m-%d.json')
    
    logging.config.dictConfig(config)
    logger = logging.getLogger()

    install_mp_handler(logger)
    
    logger.info("Intitiating program with run_id : " + str(RUN_ID))

    __method_to_excute = {
        "liveYfinanaceTick": getLiveTickData
    }
    
    try:
        if len(sys.argv) != 3:
            raise TypeError

        mthd = sys.argv[1].strip()
        conf_key = sys.argv[2].strip()

        if mthd not in __method_to_excute.keys():
            msg = "List of operation mentioned in dictionary for this package"
            raise KeyError
        else:
            logger.info(f"Operation {mthd} exists. Validating other input")
        
        if (conf_key.split('.')[0] not in fetch_prop().keys()) or (conf_key.split('.')[1] not in fetch_prop()[conf_key.split('.')[0]]):
            msg = f"key:{conf_key.split('.')[0]} and value: {conf_key.split('.')[1]} pair does not exists in tables.yml"
            raise KeyError
        else:
            logger.info(f"Configuration found in tables.yml for {conf_key}")
            logger.info("Config found for this operation from tables.yml are as below:")
            logger.info(f"\n{pformat(fetch_prop()[conf_key.split('.')[0]], indent=4)}")

        return __method_to_excute[mthd](conf_key)
    except TypeError:
        logger.critical(f"main() function takes exactly 2 arguments ({len(sys.argv[1:])} given)")
        sleep(1)
        sys.exit(1)
    except KeyError:
        logger.critical(f'Key not found: {msg}')
        sleep(1)
        sys.exit(1)


if __name__ == '__main__':
    main()
