# Station Processing Script

This Python script processes National Water Information System (NWIS) stations, calculates upstream geometries, and computes various catchment statistics. It utilizes parallel processing and Dask for efficient computation.

It uses modified versions of the processing scripts found here https://github.com/CIROH-UA/NGIAB_data_preprocess. These local versions have the extra forcing variables removed to speed up the processing time 4x and use the NWIS_site_id as the naming for folders.

## Requirements 
```bash
python -m venv env
sourc env/bin/activate
pip install -r requirements.txt
python -m map_app # you only need to run this to trigger the automatic download of conus.gpkg and model_attributes.parquet
```
Make sure the csv has a column called NWIS_site_id that contains the gauge id e.g. 01004500

## Getting zonal stats

1. Ensure all required dependencies are installed.
2. Place your input CSV file named `final_stations(all).csv` in the same directory as the script. (or modify the __main__ function of the script).
3. Run the script:

```
python csv_to_geodataframe.py
```

4. The script will generate two output files:
   - `stations.parquet`: Contains processed station data with geometries
   - `stations_with_stats.parquet`: Contains processed station data with additional catchment statistics

## Getting forcings
This will take a lot longer and depend on your download speed and number of cores. Beyond 10 cores, it will mostly be dependent on your download speed.
This scrip will use up a lot of disk space as it saves the forcing inputs for each year. 
To reduce disk usage `rm desired/output/folder/*/merged_data.nc`

```bash
echo "desired/output/folder/" >> ~/.NGIAB_data_preprocess
python add_forcings.py
```


## Note

This script is designed to run on a system with resources (96 cores, 180GB RAM). Adjust the `total_cores`, `total_memory`, and `workers` variables if running on a different system.

## Temp folder
This folder get's populated with files named after the gauge id and the contents are every catchment upstream of that gauge, and their model input parameters. mean elevation, azimuth, soil type, and many more