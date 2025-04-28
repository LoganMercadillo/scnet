"""Scrapes cable data from Telegeography's submarinecablemap.com. 

Collects

    - id
    - is_planned
    - landing_points
    - length
    - notes
    - owners
    - rfs
    - rfs_year
    - suppliers
    - url

from https://submarinecablemap.com.

Some cables lack complete data, most often in length and url categories.
"""
import asyncio
import aiometer
import functools
import httpx
import requests
import logging
import time
import datetime
import sys
import os
from pathlib import Path
from uuid import uuid4
from json import dump


SCM_BASE_URL = "https://www.submarinecablemap.com"
SCM_API = "/api/v3/"
SCRAPER_VERSION = 2.0


def init_logger(date, scraper_name, uuid):
    """Prepare and return a logger named name.

    Writes levels >= INFO to stdout and all levels (levels >= DEBUG) to log_path.

    Borrows some code from https://rb.gy/kao10.
    """
    # Create the path and parent directories for the log files
    log_path = Path("./update/logs/" + scraper_name + "_" + date + "_" + uuid + ".log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Make a logger
    logger = logging.getLogger(scraper_name)
    logger.setLevel(logging.INFO)

    # Create console handler
    stream_handler = logging.StreamHandler(sys.stdout)

    # Create file handler
    file_handler = logging.FileHandler(filename=log_path, encoding="utf-8")

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(relativeCreated)d - %(name)s - %(levelname)s - %(message)s')

    # Add formatters to handlers
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add Handlers to logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


def make_json_url(cable):
    """Transform a cable name into a URL for requesting that cable's data.
    """
    # Transform cable into request for the cable's json-formatted data.
    json_url = SCM_BASE_URL + SCM_API + "cable/" + cable['id'] + ".json"
    return json_url


def scm_scraper(base_url=SCM_BASE_URL, api=SCM_API, scraper_name="scm_scraper", start_datetime=None, write_log=False,):
    """Scrapes data for all cables on submarinecablemap.com.

    Returns a dict of cable names mapped to its data.
    """
    try:
        func_start_time = time.perf_counter()
        ###############
        #### SETUP ####
        ###############
        # Uniq ID for this run of the scraper. Currently only using the first 8 characters.
        uuid = str(uuid4().hex)[:8]

        # Set up the logger.
        if write_log:
            # Create the logger
            logger = init_logger(date=start_datetime, scraper_name=scraper_name, uuid=uuid)
            # log the start of the process.
            logger.info(msg=f"RUNNING SCM_SCRAPER_V{SCRAPER_VERSION} INSTANCE {uuid}.")

        ##################################
        #### PREPARE TO SEND REQUESTS ####
        ##################################
        # Get urls to all cables on the site.
        all_cables = requests.get(url=base_url + api + "cable/all.json")
        all_cables = all_cables.json()

        if write_log:
            logger.info(msg=f"Got list of cables. Example: {all_cables[0]['name']}")
            # data_creation_time is when the data was last updated by Telegeography (I think?)
            data_creation_time = requests.get(url=base_url + api + "config.json").json()["creation_time"]
            logger.info(msg=f"Data creation time is {data_creation_time}")

        # Build request URLs.
        json_urls = [make_json_url(c) for c in all_cables]
        if write_log:
            logger.info(msg=f"Made json urls. Example: {json_urls[0]}")

        # Set custom headers
        headers = { 
            "accept": "*/*",
            "accept-language":"en-GB,en-US;q=0.9,en;q=0.8,es;q=0.7",
            "cache-control": "no-cache",
            "referer": "https://www.submarinecablemap.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "content-type": "application/json"
        }

        #######################
        #### SEND REQUESTS ####
        #######################
        # Collect cable data in one dictionary.
        cables = {}

        # # Send the requests.
        if write_log:
            logger.info(msg="Requesting list of cable urls".format(request_start_time:=time.perf_counter()))

        async def fetch(client, url):
            response = await client.get(url, headers=headers)
            return response.json()

        async def main(urls):
            async with httpx.AsyncClient() as client:
                async with aiometer.amap(
                    functools.partial(fetch,client),
                    urls,
                    # the below settings got me the fastest time's I've had!
                    # max_at_once=500, max_per_second=250
                    # 0:00:03.478834
                    # 0:00:03.512974
                    # 0:00:03.648152
                    # 0:00:03.891524
                    max_at_once=500,
                    max_per_second=250
                ) as responses:
                    async for r in responses:
                        #########################
                        #### STORE RESPONSES ####
                        #########################
                        try:
                            cable_request_time = format((time.perf_counter() - request_start_time), '.3f')
                            cable_name = r.pop("name")
                            cables[cable_name] = r

                            if write_log:
                                logger.info(msg=f"Collected data for {cable_name} in {cable_request_time} seconds.")

                        except Exception as e:
                            # TODO: Better exception handling for cable response errors.
                            print(e)
                            if write_log:
                                logger.error(e, exc_info=True)
                            continue

        asyncio.run(main(json_urls))

        ###########################
        #### FINISH AND RETURN ####
        ###########################
        # Finish writing the log, return the data.
        if write_log:
            logger.info(msg="Received responses to requests for each cable's data."
                        .format(cable_total_time:=datetime.timedelta(seconds=(time.perf_counter() - request_start_time)))
            )
            logger.info(msg=f"SCM_SCRAPER_V{SCRAPER_VERSION} INSTANCE {uuid} completed."
                        + " " +
                        f"Function execution time: {datetime.timedelta(seconds=(time.perf_counter() - func_start_time))}."
                        + " " +
                        f"Cable bulk request response time: {cable_total_time}."
            )
            # Each data file name should end with
            # the scraper start datetime and the unique identifier.
            return (cables, start_datetime + "_" + uuid)

        return cables

    except Exception as e:
        print(e)
        if write_log:
            logger.error(e, exc_info=True)


def main():
    # Datetime of run, formatted like '2025-04-27T14:31:11.854'
    start_datetime = datetime.datetime.now().isoformat(timespec='milliseconds')
    # For tracking scraper function performance.
    outside_scraper_start_time = time.perf_counter()

    # Scrape the data
    scm_data, file_name = scm_scraper(start_datetime=start_datetime, write_log=True)

    # Record how long the scraper took to run
    outside_scraper_end_time = time.perf_counter()
    outside_scraper_elapsed_time = datetime.timedelta(seconds=(outside_scraper_end_time - outside_scraper_start_time))
    print(f"Outside scraper performance: {outside_scraper_elapsed_time}")

    # Build the path and name of the output file.
    data_path = Path("./update/data/scm_data_" + file_name + ".json")
    data_path.parent.mkdir(parents=True, exist_ok=True)

    # Create the output file and write the scraper output.
    with open(data_path, "wt", encoding="utf-8") as f:
        try:
            dump(scm_data, f, ensure_ascii=False, sort_keys=True, indent=4)
            print(f"Wrote scm cable data to {data_path}.")
        except (TypeError, Exception) as exc:
            print(exc)
            print("Could not write scm cable data.")
            print(f"Log Number: {num}\n")
            print(f"Cables:\n\n {scm_data}")


if __name__ == '__main__':
    main()
