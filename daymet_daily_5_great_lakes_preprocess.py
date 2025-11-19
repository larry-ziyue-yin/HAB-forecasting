from pathlib import Path

import geopandas as gpd
import xarray as xr
import numpy as np
import pandas as pd

from shapely import geometry
from shapely import vectorized  # shapely.vectorized.contains

# paths
ROOT = Path("/dkucc/home/zy166/HAB-forecasting")

GPKG = ROOT / "datasets/Lakes/shapes/lakes_greatlakes_5poly.gpkg"
DAYMET_DIR = ROOT / "datasets/Daymet/2024_north_america_daily"
OUT_DAYMET_DAILY = ROOT / "datasets/Daymet/daymet_glakes_daily.parquet"

# lakes vector, reproject to EPSG:4326 (lat/lon)
lakes = gpd.read_file(GPKG)
lakes = lakes.to_crs(4326)

if "lake_id" not in lakes.columns:
    raise ValueError("Expect 'lake_id' in lakes_greatlakes_5poly.gpkg")

# find a name column, not required
name_col = None
for cand in ["lake_name", "Lake_name", "name", "Name"]:
    if cand in lakes.columns:
        name_col = cand
        break
if name_col is None:
    lakes["lake_name"] = lakes["lake_id"]
    name_col = "lake_name"

lakes = lakes[["lake_id", name_col, "geometry"]]

print(lakes)

# Use one file (e.g., tmin) to get the lat/lon grid.
f_grid = DAYMET_DIR / "daymet_v4_daily_na_tmin_2024.nc"

# Use dask chunk to reduce the memory pressure (about 1km grid of North America)
ds_grid = xr.open_dataset(
    f_grid,
    chunks={"time": 365, "y": 1000, "x": 1000},
)

print(ds_grid)

# According to the Daymet documentation, there are usually `lat(y,x)` and `lon(y,x)`
# If the names are not exactly these two, you can print ds_grid.data_vars / ds_grid.coords to see the exact names
lat = ds_grid["lat"].values  # shape: (ny, nx)
lon = ds_grid["lon"].values  # shape: (ny, nx)

ny, nx = lat.shape
print(f"[INFO] Daymet grid shape: ny={ny}, nx={nx}")

# Prepare a mask DataArray for each lake
lake_masks = {}  # lake_id -> xr.DataArray(bool[y,x])

for _, row in lakes.iterrows():
    lake_id   = row["lake_id"]
    lake_name = row[name_col]
    geom      = row.geometry

    # Some MultiPolygon / topology may have problems, buffer(0)一下
    if not isinstance(geom, geometry.base.BaseGeometry):
        geom = geometry.shape(geom)
    geom = geom.buffer(0)

    print(f"[INFO] build mask for lake {lake_id} ({lake_name}) ...")

    # shapely.vectorized.contains expects (geom, x, y) = (polygon, lon, lat)
    mask_bool = vectorized.contains(geom, lon, lat)  # shape: (ny, nx) bool

    if not mask_bool.any():
        print(f"[WARN] mask for lake {lake_id} has no True pixels, check geometry/CRS")
    lake_masks[lake_id] = xr.DataArray(
        mask_bool,
        dims=("y", "x"),
        coords={"y": ds_grid["y"], "x": ds_grid["x"]},
        name=f"mask_{lake_id}",
    )

print(f"[OK] built masks for {len(lake_masks)} lakes")
ds_grid.close()


# Map variable names to file names
VAR_FILES = {
    "tmin": "daymet_v4_daily_na_tmin_2024.nc",
    "tmax": "daymet_v4_daily_na_tmax_2024.nc",
    "prcp": "daymet_v4_daily_na_prcp_2024.nc",
    "srad": "daymet_v4_daily_na_srad_2024.nc",
    "vp":   "daymet_v4_daily_na_vp_2024.nc",
    "dayl": "daymet_v4_daily_na_dayl_2024.nc",
}

def compute_lake_daily_for_var(var_name: str) -> pd.DataFrame:
    """
    Compute the daily mean time series of each lake for a Daymet variable (e.g., tmin).
    Return columns: ['lake_id', 'date', var_name]
    """
    fpath = DAYMET_DIR / VAR_FILES[var_name]
    print(f"[INFO] open {var_name} from {fpath}")

    ds = xr.open_dataset(
        fpath,
        chunks={"time": 30, "y": 1000, "x": 1000},
    )
    # The variable name is usually the same as the file name, if not, change it here
    da = ds[var_name]  # dims: time, y, x

    rows = []

    for _, row in lakes.iterrows():
        lake_id   = row["lake_id"]
        lake_name = row[name_col]
        mask_da   = lake_masks[lake_id]  # bool[y,x]

        print(f"[INFO] compute {var_name} daily mean for lake {lake_id} ({lake_name}) ...")

        # broadcast mask to (time, y, x), only keep the pixels inside the lake
        masked = da.where(mask_da)

        # Take the mean of the spatial dimensions, get the time series
        series = masked.mean(dim=("y", "x"), skipna=True)  # DataArray(time)

        df = series.to_dataframe(name=var_name).reset_index()  # columns: ['time', var_name]
        df["lake_id"]   = lake_id
        df["lake_name"] = lake_name
        rows.append(df)

    ds.close()
    out = pd.concat(rows, ignore_index=True)
    # Use the same name for the time column: date (no timezone)
    out["date"] = pd.to_datetime(out["time"], utc=True).dt.tz_localize(None)
    out = out.drop(columns=["time"])

    return out[["lake_id", "lake_name", "date", var_name]]


# Get a `df` for each variable, then merge them step by step
dfs = []
for var_name in VAR_FILES.keys():
    df_var = compute_lake_daily_for_var(var_name)
    dfs.append(df_var)

# Use the first variable as the base, then merge with other variables step by step
from functools import reduce

df_daymet = reduce(
    lambda left, right: pd.merge(
        left,
        right,
        on=["lake_id", "lake_name", "date"],
        how="outer",
    ),
    dfs,
)

# Sort it for better readability
df_daymet = df_daymet.sort_values(["lake_id", "date"]).reset_index(drop=True)

print(df_daymet.head())
print(df_daymet.describe(include="all"))

# Save as parquet
OUT_DAYMET_DAILY.parent.mkdir(parents=True, exist_ok=True)
df_daymet.to_parquet(OUT_DAYMET_DAILY, index=False)
print(f"[OK] saved daily lake-mean Daymet → {OUT_DAYMET_DAILY}, rows={len(df_daymet)}")