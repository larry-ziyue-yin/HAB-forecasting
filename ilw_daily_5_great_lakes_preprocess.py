import re
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import rioxarray  # NOTE: currently not used in clipping, but kept for future use
import time

def infer_time_label(nc_path, ds, product="monthly"):
    """
    Return a pandas.Timestamp, try to infer from ds or filename.
    product: 'monthly' or 'daily'
    """
    # 1) Directly have time coordinate/variable
    if "time" in ds.coords or "time" in ds.variables:
        try:
            tt = pd.to_datetime(ds["time"].values)
            tt = np.array(tt).reshape(-1)[0]
            return pd.to_datetime(tt)
        except Exception:
            pass

    # 2) Global attributes (common in L3M data)
    start = ds.attrs.get("time_coverage_start") or ds.attrs.get("start_time")
    end   = ds.attrs.get("time_coverage_end")   or ds.attrs.get("end_time")
    if start and end:
        try:
            ts = pd.to_datetime(start)
            te = pd.to_datetime(end)
            if product == "monthly":
                return ts + (te - ts) / 2
            else:
                return ts
        except Exception:
            pass

    # 3) Analyze the filename
    fn = nc_path.split("/")[-1]
    if product == "monthly":
        # ...YYYYMMDD_YYYYMMDD.L3m.MO...
        m = re.search(r"\.(\d{8})_(\d{8})\.L3m\.MO\.", fn)
        if m:
            b, e = m.group(1), m.group(2)
            ts = pd.to_datetime(b, format="%Y%m%d")
            te = pd.to_datetime(e, format="%Y%m%d")
            return ts + (te - ts) / 2
    else:
        # ...YYYYMMDD.L3m.DAY...
        m = re.search(r"\.(\d{8})\.L3m\.DAY\.", fn)
        if m:
            return pd.to_datetime(m.group(1), format="%Y%m%d")

    raise ValueError("Cannot infer time from dataset or filename: " + fn)

def clean_ci(da: xr.DataArray) -> xr.DataArray:
    """
    Filter out values out of physical range and remove near-zero values.
    """
    vmin = float(da.attrs.get("valid_min", np.nan))
    vmax = float(da.attrs.get("valid_max", np.nan))
    if np.isfinite(vmin):
        da = da.where(da >= vmin)
    if np.isfinite(vmax):
        da = da.where(da <= vmax)

    # drop near-zero (background) CI
    thr = max(vmin, 5e-5) if np.isfinite(vmin) else 5e-5
    da = da.where(da > thr)

    return da

def _extract_lakes_core_from_ds(
    ds: xr.Dataset,
    nc_path: str,
    lakes_gdf: gpd.GeoDataFrame,
    lake_id_col: str,
    product: str,
    time_label: pd.Timestamp,
    engine: str,
) -> pd.DataFrame:
    """
    核心提取逻辑：针对一个 xarray.Dataset，用 lat/lon + bbox 做掩膜裁剪 CI_cyano。
    兼容曲线网格(lat(y,x), lon(y,x))。
    返回列：
      lake_id, time, product, CI_mean, CI_p90, n_valid, src, engine
    """
    if "CI_cyano" not in ds.data_vars:
        raise KeyError("CI_cyano not found in dataset data_vars")

    da = ds["CI_cyano"]
    da = clean_ci(da)

    # 期望 lat, lon 是 2D (y,x)，与 CI_cyano 维度一致
    if "lat" not in ds.variables or "lon" not in ds.variables:
        raise KeyError("lat / lon not found in dataset")

    lat = ds["lat"]
    lon = ds["lon"]

    if lat.shape != da.shape or lon.shape != da.shape:
        # 极端情况下可以做广播，但对 ILW_CONUS 来说应该是一致的
        raise ValueError(
            f"lat/lon shape {lat.shape}/{lon.shape} not matching CI_cyano {da.shape}"
        )

    rows = []
    for _, r in lakes_gdf.iterrows():
        lid = r[lake_id_col]
        geom = r.geometry
        minx, miny, maxx, maxy = geom.bounds  # 经度, 纬度

        # 使用 bbox 对 lat/lon 作初筛
        mask = (
            (lon >= minx) & (lon <= maxx) &
            (lat >= miny) & (lat <= maxy)
        )

        sub = da.where(mask)
        arr = np.asarray(sub.values)
        finite = np.isfinite(arr)
        n_valid = int(finite.sum())

        if n_valid > 0:
            vals = arr[finite].ravel()
            mean_val = float(np.nanmean(vals))
            p90      = float(np.nanquantile(vals, 0.9))
        else:
            mean_val, p90 = np.nan, np.nan

        rows.append({
            "lake_id": lid,
            "time":   pd.to_datetime(time_label),
            "product": product,
            "CI_mean": mean_val,
            "CI_p90":  p90,
            "n_valid": n_valid,
            "src":     Path(nc_path).name,
            "engine":  engine,
        })

    return pd.DataFrame(rows)

def extract_lakes_from_nc(nc_path: str,
                          lakes_gdf: gpd.GeoDataFrame,
                          lake_id_col: str,
                          product: str) -> pd.DataFrame:
    """
    nc_path: A single NetCDF file (S3B monthly or S3M daily)
    lakes_gdf: A GeoDataFrame containing `lake_id` and `geometry` (EPSG:4326)
    product: 'monthly' | 'daily'
    Returns: One row per lake (timestamp of the file)
    """
    nc_path = str(nc_path)
    with xr.open_dataset(nc_path, engine="netcdf4") as ds:
        t = infer_time_label(nc_path, ds, product=product)
        df = _extract_lakes_core_from_ds(
            ds=ds,
            nc_path=nc_path,
            lakes_gdf=lakes_gdf,
            lake_id_col=lake_id_col,
            product=product,
            time_label=t,
            engine="netcdf4",
        )
    return df

def set_spatial_dims_safe(da: xr.DataArray, ds: xr.Dataset | None = None) -> xr.DataArray:
    """
    [遗留函数] 现在不再用于 lake 提取，但保留以兼容其他代码。
    Make `da` geospatially aware for rioxarray:
    - Prefer real dimension names present on `da` (lon/lat or x/y or variants);
    - Only pass EXISTING dimension names to `rio.set_spatial_dims`;
    - If dataset carries 1D lon/lat arrays matching x/y lengths, bind them as coords;
    - Finally write CRS=EPSG:4326.

    NOTE: If lon/lat are 2D (curvilinear), we DO NOT pass them as dims; we keep x/y dims.
    """
    dims = list(da.dims)

    # normalize common variants
    def _has(*cands):
        return any(c in dims for c in cands)

    # Case A: dims already lon/lat
    if "lon" in dims and "lat" in dims:
        out = da.rio.write_crs(4326)
        out = out.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)
    elif "longitude" in dims and "latitude" in dims:
        out = da.rename({"longitude": "lon", "latitude": "lat"})
        out = out.rio.write_crs(4326)
        out = out.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)

    # Case B: dims are x/y (any case)
    elif (_has("x") and _has("y")) or (_has("X") and _has("Y")):
        # rename upper-case to lower-case to please rioxarray
        rename_map = {}
        if "X" in dims: rename_map["X"] = "x"
        if "Y" in dims: rename_map["Y"] = "y"
        out = da.rename(rename_map) if rename_map else da
        out = out.rio.write_crs(4326)
        out = out.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)

        # try binding 1D lon/lat coords from ds if lengths match
        if ds is not None:
            try:
                lon1d = None
                lat1d = None
                for lon_name in ("lon","longitude"):
                    if lon_name in ds.variables and ds[lon_name].ndim == 1 and ds[lon_name].sizes[ds[lon_name].dims[0]] == out.sizes["x"]:
                        lon1d = np.asarray(ds[lon_name].values)
                        break
                for lat_name in ("lat","latitude"):
                    if lat_name in ds.variables and ds[lat_name].ndim == 1 and ds[lat_name].sizes[ds[lat_name].dims[0]] == out.sizes["y"]:
                        lat1d = np.asarray(ds[lat_name].values)
                        break
                if lon1d is not None and lat1d is not None:
                    out = out.assign_coords(x=lon1d, y=lat1d)
            except Exception:
                pass

    # Case C: unknown names → rename the last two dimensions to y/x
    elif len(dims) >= 2:
        ydim, xdim = dims[-2], dims[-1]
        out = da.rename({xdim: "x", ydim: "y"}).rio.write_crs(4326)
        out = out.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=False)

    else:
        raise ValueError(f"Cannot determine spatial dims for {da.name!r}; dims={dims}")

    return out

def looks_like_hdf5(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
        return sig.startswith(b"\x89HDF") or sig.startswith(b"CDF")
    except Exception:
        return False

def _extract_one_with_h5netcdf(nc_path: Path,
                               lakes_gdf: gpd.GeoDataFrame,
                               lake_id_col: str,
                               product: str = "daily") -> pd.DataFrame:
    """
    兜底方案：用 h5netcdf 打开并在本函数内完成裁剪与统计。
    现在同样使用 lat/lon + bbox 掩膜，不再调用 rioxarray.clip。
    返回列同 extract_lakes_from_nc：
      lake_id, time, product, CI_mean, CI_p90, n_valid, src, engine
    """
    nc_path = Path(nc_path)
    with xr.open_dataset(nc_path, engine="h5netcdf", phony_dims="access") as ds:
        t = infer_time_label(str(nc_path), ds, product=product)
        df = _extract_lakes_core_from_ds(
            ds=ds,
            nc_path=str(nc_path),
            lakes_gdf=lakes_gdf,
            lake_id_col=lake_id_col,
            product=product,
            time_label=t,
            engine="h5netcdf",
        )
    return df

def try_open_xarray(fp: Path):
    """Try netcdf4 → h5netcdf → h5py，return (ds or None, engine_used)。"""
    try:
        ds = xr.open_dataset(fp, engine="netcdf4", chunks="auto")
        _ = ds.dims
        return ds, "netcdf4"
    except Exception as e1:
        try:
            ds = xr.open_dataset(fp, engine="h5netcdf", chunks="auto", phony_dims="access")
            _ = ds.dims
            return ds, "h5netcdf"
        except Exception as e2:
            try:
                import h5py
                with h5py.File(fp, "r") as f:
                    pass
                print(f"[WARN] {fp.name} readable by h5py but not by xarray engines (netCDF-4 layout issue?)")
            except Exception as e3:
                print(f"[WARN] {fp.name} not readable even by h5py: {e3}")
            print(f"[SKIP] {fp.name} → netcdf4:{e1} | h5netcdf:{e2}")
            return None, None
        

def _log_one_file(df_one, fp_name, engine, t0):
    """在处理完一个 nc 文件后打印一行进度日志。"""
    elapsed = time.time() - t0

    # 取日期列：优先 date，其次 time
    date_col = None
    if "date" in df_one.columns:
        date_col = "date"
    elif "time" in df_one.columns:
        date_col = "time"

    if date_col is not None:
        uniq = pd.to_datetime(df_one[date_col].dropna().unique())
        if len(uniq) == 1:
            date_str = uniq[0].strftime("%Y-%m-%d")
        elif len(uniq) > 1:
            dmin, dmax = uniq.min(), uniq.max()
            date_str = f"{dmin.strftime('%Y-%m-%d')}→{dmax.strftime('%Y-%m-%d')} (n={len(uniq)})"
        else:
            date_str = "NA"
    else:
        date_str = "NA"

    n_lakes = df_one["lake_id"].nunique() if "lake_id" in df_one.columns else "NA"

    print(
        f"[daily] done {fp_name} | date={date_str} | rows={len(df_one)} | "
        f"lakes={n_lakes} | engine={engine} | {elapsed:.1f}s",
        flush=True,
    )


def run_daily(daily_dir: str,
              lakes_fp: str,
              lake_id_col: str,
              out_parquet: str):
    """
    daily_dir: Directory containing files like S3M_OLCI_EFRNT.*.L3m.DAY.ILW_CONUS...nc
    """
    daily_dir = Path(daily_dir)
    gdf = gpd.read_file(lakes_fp)
    if gdf.crs is None:
        raise ValueError("The lake file is missing CRS, please ensure it is EPSG:4326")
    gdf = gdf.to_crs(4326)[[lake_id_col, "geometry"]].dropna()

    out_rows = []
    files = sorted(daily_dir.glob("S3M_OLCI_EFRNT.*.L3m.DAY.*.nc"))
    if not files:
        print(f"[WARN] No daily files found under: {daily_dir}")
        return

    print(f"[daily] found {len(files)} files under {daily_dir}")
    for fp in files:
        t0 = time.time()
        print(f"[daily] start {fp.name}", flush=True)

        # 1) 文件头快速校验
        if not looks_like_hdf5(fp):
            print(f"[WARN] Skip (not HDF5/NetCDF header): {fp.name}")
            continue

        # 2) 首选：netcdf4 引擎
        try:
            df_one = extract_lakes_from_nc(str(fp), gdf, lake_id_col, product="daily")
            # 统一列名为 date
            if "time" in df_one.columns and "date" not in df_one.columns:
                df_one = df_one.rename(columns={"time": "date"})
            df_one["src"] = fp.name
            df_one["engine"] = "netcdf4"
            out_rows.append(df_one)
            _log_one_file(df_one, fp.name, "netcdf4", t0)
            continue
        except Exception as e1:
            print(f"[WARN] netcdf4 failed on {fp.name}: {e1}")

        # 3) 兜底：h5netcdf 内联提取
        try:
            df_one = _extract_one_with_h5netcdf(fp, gdf, lake_id_col, product="daily")
            if "time" in df_one.columns and "date" not in df_one.columns:
                df_one = df_one.rename(columns={"time": "date"})
            df_one["src"] = fp.name
            df_one["engine"] = "h5netcdf"
            out_rows.append(df_one)
            _log_one_file(df_one, fp.name, "h5netcdf", t0)
        except Exception as e2:
            print(f"[SKIP] {fp.name}: h5netcdf fallback failed → {e2}")

    if not out_rows:
        print("No valid daily rows produced.")
        return

    df_all = pd.concat(out_rows, ignore_index=True)

    # 基本整理：日期排序、列顺序、小型诊断
    keep_cols = ["lake_id", "date", "product", "CI_mean", "CI_p90", "n_valid", "src", "engine"]
    for c in keep_cols:
        if c not in df_all.columns:
            # lake_id/product/src/engine 用 None，其余用 NaN
            df_all[c] = np.nan if c not in ("lake_id", "product", "src", "engine") else None
    df_all = df_all[keep_cols].sort_values(["lake_id", "date"]).reset_index(drop=True)

    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(out_parquet, index=False)
    print(f"[daily] saved → {out_parquet}  (rows={len(df_all)}, files={len(files)})", flush=True)


if __name__ == "__main__":
    run_daily(
        daily_dir="/dkucc/home/zy166/HAB-forecasting/datasets/ILW/Merged/2024/CONUS_DAY",
        lakes_fp="/dkucc/home/zy166/HAB-forecasting/datasets/Lakes/shapes/lakes_greatlakes_5poly.gpkg",
        lake_id_col="lake_id",
        out_parquet="/dkucc/home/zy166/HAB-forecasting/datasets/processed/lake_ci_daily.parquet"
    )