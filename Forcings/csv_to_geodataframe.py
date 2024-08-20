import multiprocessing as mp
import os
import sqlite3
from collections import defaultdict
from typing import List

import dask
import geopandas as gpd
import pandas as pd
import pyarrow
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster, progress
from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import get_cat_from_gage_id, get_table_crs
from data_processing.graph_utils import get_upstream_ids
from map_app.views import get_upstream_geometry, get_catid_from_point
from pyarrow import compute as pa_compute
from pyarrow import csv as pa_csv
from pyarrow import parquet as pa_parquet
from shapely.wkb import loads


def process_station(row):
    # get the station
    station_id = row["NWIS_site_id"]
    try:
        catids = get_cat_from_gage_id(station_id)
        upstream_ids = get_upstream_ids(catids)
        return get_upstream_geometry(upstream_ids)
    except TypeError:
        # lng lat
        lat = row["dec_lat_va"]
        lng = row["dec_long_va"]
        coords = {"lat": lat, "lng": lng}
        cat_id = get_catid_from_point(coords)
        upstream_ids = get_upstream_ids([cat_id])
        return get_upstream_geometry(upstream_ids)
    except:
        return None


def get_catchment_stats(ids: List[str], temp_file_name) -> None:
    # check if the file exists before writing
    if os.path.exists(f"./temp/{temp_file_name}.csv"):
        return
    cat_ids = [x.replace("wb", "cat") for x in ids]
    parquet_path = file_paths.model_attributes()
    table = pa_parquet.read_table(parquet_path)
    filtered_table = table.filter(
        pa_compute.is_in(table.column("divide_id"), value_set=pyarrow.array(cat_ids))
    )
    pa_csv.write_csv(filtered_table, f"./temp/{temp_file_name}.csv")
    del table
    del filtered_table


def get_catchment_areas(upstream_ids):
    # takes the upstream_ids and returns a dictionary of the areas of the catchments
    geopackage = file_paths.conus_hydrofabric()
    sql_query = f"SELECT divide_id, areasqkm FROM divides WHERE id IN {tuple(upstream_ids)}"
    sql_query = sql_query.replace(",)", ")")
    with sqlite3.connect(geopackage) as con:
        result = con.execute(sql_query).fetchall()
    areas = {}
    for r in result:
        areas[r[0]] = r[1]
    return areas


def get_hf_id_from_gage_id(gage_id):
    geopackage = file_paths.conus_hydrofabric()
    sql_query = f"SELECT hf_id FROM network WHERE hl_uri = 'Gages-{gage_id}'"
    with sqlite3.connect(geopackage) as con:
        result = con.execute(sql_query).fetchall()
    # convert to string and remove trailing .0
    hf_ids = [str(r[0])[:-2] for r in result]
    return hf_ids


def get_stats_geometry(row, upstream_ids):

    temp_file_name = row["NWIS_site_id"]
    catchment_areas = get_catchment_areas(upstream_ids)
    total_area = sum(catchment_areas.values())

    get_catchment_stats(upstream_ids, temp_file_name)
    cat_params = pd.read_csv(f"./temp/{temp_file_name}.csv")
    soil_types = defaultdict(int)
    veg_types = defaultdict(int)
    for cat_id, area in catchment_areas.items():
        soil_type = cat_params.loc[cat_params["divide_id"] == cat_id, "ISLTYP"].values[0]
        veg_type = cat_params.loc[cat_params["divide_id"] == cat_id, "IVGTYP"].values[0]
        soil_types[soil_type] += area
        veg_types[veg_type] += area

    basin_mean_elevation = cat_params["elevation_mean"].mean()
    basin_mean_slope = cat_params["slope_mean"].mean()
    new_row = {
        "NWIS_site_id": row["NWIS_site_id"],
        "nhdplus_id": get_hf_id_from_gage_id(row["NWIS_site_id"]),
        "total_area": total_area,
        "basin_mean_elevation": basin_mean_elevation,
        "basin_mean_slope": basin_mean_slope,
    }
    num_soil_types = 19
    num_veg_types = 27
    for i in range(1, num_soil_types + 1):
        soil_types[i] += 0
        new_row[f"soil_type_{i}"] = soil_types[i]
        new_row[f"soil_type_{i}_perc"] = soil_types[i] / total_area
    for i in range(1, num_veg_types + 1):
        veg_types[i] += 0
        new_row[f"veg_type_{i}"] = veg_types[i]
        new_row[f"veg_type_{i}_perc"] = veg_types[i] / total_area
    return new_row


def process_station2(row):
    # get the station
    station_id = row["NWIS_site_id"]
    try:
        catids = get_cat_from_gage_id(station_id)
        upstream_ids = get_upstream_ids(catids)
    except TypeError:
        # lng lat
        lat = row["dec_lat_va"]
        lng = row["dec_long_va"]
        coords = {"lat": lat, "lng": lng}
        cat_id = get_catid_from_point(coords)
        upstream_ids = get_upstream_ids([cat_id])
    except:
        return None

    # Convert geometry from bytes to Shapely geometry
    if isinstance(row["geometry"], bytes):
        row["geometry"] = loads(row["geometry"])

    return get_stats_geometry(row, upstream_ids)


@dask.delayed
def process_station_delayed(row):
    return process_station2(row)


if __name__ == "__main__":
    NWIS_STATIONS = pd.read_csv("final_stations(all).csv")

    # make the NWIS_station_id a string
    NWIS_STATIONS["NWIS_site_id"] = NWIS_STATIONS["NWIS_site_id"].astype(str)

    # if the station id is less than 8 characters, add 0s to the front
    NWIS_STATIONS["NWIS_site_id"] = NWIS_STATIONS["NWIS_site_id"].apply(
        lambda x: "0" * (8 - len(x)) + x
    )
    print(f"current row count: {len(NWIS_STATIONS)}")

    # remove the .0 from the end of the station id
    NWIS_STATIONS["NWIS_site_id"] = NWIS_STATIONS["NWIS_site_id"].apply(
        lambda x: x[:-2] if x[-2:] == ".0" else x
    )
    print(NWIS_STATIONS.head())
    crs = get_table_crs(file_paths.conus_hydrofabric(), "divides")
    print(crs)

    # Use multiprocessing to process stations in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        geometries = pool.map(process_station, [row for _, row in NWIS_STATIONS.iterrows()])

    NWIS_STATIONS["geometry"] = geometries
    print(f"current row count: {len(NWIS_STATIONS)}")
    gdf = gpd.GeoDataFrame(NWIS_STATIONS, geometry="geometry", crs=crs)
    # gdf.to_crs(epsg=4326, inplace=True)
    gdf.to_crs(epsg=4326).to_parquet("stations.parquet")
    # Set up optimized Dask cluster
    total_cores = 50
    total_memory = 180e9  # 128 GB in bytes
    workers = 50
    threads_per_worker = total_cores // workers
    memory_per_worker = total_memory // workers

    cluster = LocalCluster(
        n_workers=workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_per_worker,
        silence_logs=40,
    )
    client = Client(cluster)
    print(f"Dashboard link: {client.dashboard_link}")

    # Read the parquet file using pandas (not Dask)
    # gdf = gpd.read_parquet("stations.parquet")
    crs = get_table_crs(file_paths.conus_hydrofabric(), "divides")
    # gdf = gdf.to_crs(crs=crs)
    # Create a delayed task for each row
    delayed_results = [process_station_delayed(row) for _, row in gdf.iterrows()]

    # Compute the result with progress bar
    with ProgressBar():
        print("Processing stations...")
        futures = client.compute(delayed_results)
        progress(futures)
        new_rows = client.gather(futures)

    print("Processing complete!")

    # Convert the result to a DataFrame
    new_gdf = pd.DataFrame([row for row in new_rows if row is not None])

    # Convert new_gdf to GeoDataFrame and set CRS if needed
    if "geometry" in new_gdf.columns:
        new_gdf = gpd.GeoDataFrame(new_gdf, geometry="geometry", crs=gdf.crs)

    # Merge the new stats with the original df
    new_gdf = gdf.merge(new_gdf, on="NWIS_site_id")

    new_gdf.to_crs(epsg=4326).to_parquet("stations_with_stats.parquet")

    # Close the Dask client
    client.close()
