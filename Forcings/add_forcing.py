import logging

from time import sleep

from dask.distributed import Client, LocalCluster
from helpers.forcings import create_forcings

if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-02-01 00:00"
    for i in range(1988, 2021, 1):
        start_time = f"{i}-01-01 00:00"
        end_time = f"{i+1}-01-01 00:00"
        print(start_time, end_time)
        output_folder_name = f"start_{i}_end_{i+1}"
        # looks in output/wb-1643991/config for the geopackage wb-1643991_subset.gpkg
        # puts forcings in output/wb-1643991/forcings
        # output logs to stdout
        logging.basicConfig(level=logging.INFO)
        create_forcings(start_time, end_time, output_folder_name)
        # clear out the memory of the cluster
        # break
        sleep(5)
