# HAB-forecasting: Forecasting Freshwater Algal Bloom Levels Using Multisource Climate Data

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)

This project develops an interpretable forecasting framework that combines satellite-derived cyanobacterial indices, daily gridded meteorology, and lake geometry to predict near-term bloom intensity in the Laurentian Great Lakes. The system provides 1-, 7-, and 28-day forecasts of harmful algal bloom (HAB) levels to support operational decisions in water resource management.

## Key Features
- **Multi-source data integration**: Fuses NASA Inland Waters CI products, Daymet meteorology, and lake geometry
- **Two-phase analytical design**:
  - Phase I: Climate driver analysis using linear models and random forests
  - Phase II: Multi-horizon forecasting with LOLO (Leave-One-Lake-Out) evaluation
- **Production-ready models**: Ridge regression, Random Forests, Gradient Boosting Machines
- **Reproducible pipeline**: From data ingestion to forecasting results

## Data Sources
| Source | Description | Resolution | Variables |
|--------|-------------|------------|-----------|
| **NASA Inland Waters (ILW)** | Cyanobacteria Index products | ~300m | $CI_{cyano}$, surface reflectances |
| **Daymet v4R1** | Daily gridded meteorology | 1km | Temperature, precipitation, radiation, vapor pressure |
| **NHDPlus HR** | Lake geometry | High-res | Great Lakes polygons |

Note: links to certain datasets are listed in `data_preprocess.ipynb`.

## Methodology
### Phase I: Climate Driver Analysis
- Quantifies relationships between meteorological variables and bloom intensity
- Uses standardized linear regression and random forests
- Identifies key drivers: precipitation, shortwave radiation, vapor pressure

### Phase II: Multi-horizon Forecasting
- Predicts $\log(1+CI_{p90})$ at 1-, 7-, and 28-day horizons
- Features include:
  - Seasonal terms (DOY sine/cosine)
  - Autoregressive CI features (1,7,28-day lags)
  - Rolling meteorology (7,28-day means)
  - Expanding-year summaries
- Evaluated under LOLO cross-validation

## Results
- Achieves RMSE $~6\times10^{-4}$ to $8\times10^{-4}$ on log-CI scale across horizons
- 28-day forecasts show comparable/better performance than shorter leads
- Key findings:
  - Target smoothing improves longer-horizon predictability
  - Observation noise limits short-lead accuracy
  - Cross-lake generalization varies with data quality

## Dependencies
__*TL;DR:*__ simply `conda env create -f environment.yml` will do.

Some core Python libraries are:
- `xarray`, `netCDF4` (satellite data processing)
- `geopandas`, `shapely` (spatial operations)
- `pandas`, `numpy` (data manipulation)
- `scikit-learn` (machine learning)
- `matplotlib`, `seaborn` (visualization)

## Getting Started
1. Clone repository:
    ```bash
    git clone https://github.com/larry-ziyue-yin/HAB-forecasting.git
    cd HAB-forecasting
    ```

2. Install dependencies (recommended in virtual environment):
   ```bash
   conda env create -f environment.yml
   ```

3. Example usages are given within the scripts.

4. Execute Jupyter notebooks in order if you wish:
    - `data_preprocess.ipynb`
    - `model_phase_1_feature_importance.ipynb`
    - `model_phase_2_cyanobacteria_forecasting.ipynb`