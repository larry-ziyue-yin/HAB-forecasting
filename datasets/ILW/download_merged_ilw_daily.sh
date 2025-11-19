#!/usr/bin/env bash
set -euo pipefail

# 用法：
#   bash download_merged_ilw_daily.sh 2024 CONUS
#   bash download_merged_ilw_daily.sh 2021 ALASKA 2021-03-01 2021-05-31   # 可选起止日期
YEAR="${1:-2024}"
REG="${2:-CONUS}"                 # CONUS | ALASKA
START_DATE="${3:-${YEAR}-01-01}"  # 可选，默认当年1月1日
END_DATE="${4:-$((${YEAR}+1))-01-01}"  # 可选，默认到次年1月1日（不含）

BASE="https://oceandata.sci.gsfc.nasa.gov/ob/getfile"
COOKIE="${HOME}/.urs_cookies"

# 区域尾缀（文件名最后一段不同）
if [[ "$REG" == "CONUS" ]]; then
  TAIL="ILW_CONUS.V5.all.CONUS.300m.nc"
else
  TAIL="ILW_ALASKA.V5.all.ILW_AK.300m.nc"
fi

OUT="Merged/${YEAR}/${REG}_DAY"
mkdir -p "$OUT"

echo "==> Merged-S3-ILW DAILY  $REG  $YEAR  →  $OUT"
cur="$START_DATE"
while [[ "$cur" < "$END_DATE" ]]; do
  ymd="$(date -d "$cur" +%Y%m%d)"      # YYYYMMDD
  file="S3M_OLCI_EFRNT.${ymd}.L3m.DAY.${TAIL}"
  url="${BASE}/${file}"
  dest="${OUT}/${file}"

  # 取“最终状态码”（跟随重定向）
  code=$(curl -s -L -n -c "$COOKIE" -b "$COOKIE" -o /dev/null -w '%{http_code}' "$url")
  if [[ "$code" != "200" ]]; then
    echo "miss  $file   (HTTP $code)"
  else
    if [[ -s "$dest" ]]; then
      echo "skip  $file"
    else
      echo "get   $file"
      curl -L -n -C - --retry 3 --retry-delay 2 \
           -c "$COOKIE" -b "$COOKIE" \
           -o "$dest" "$url"
    fi
  fi
  cur="$(date -d "$cur +1 day" +%Y-%m-%d)"
done

echo "Done."