#!/usr/bin/env bash
set -euo pipefail

# 用法：
#   bash download_merged_ilw_monthly.sh 2024 CONUS
#   bash download_merged_ilw_monthly.sh 2020-2024 ALASKA 03 10   # 多年 + 选取3-10月
YEARS="$1"                 # 形如 "2024" 或 "2020-2024"
REG="${2:-CONUS}"          # CONUS | ALASKA
START_M="${3:-01}"         # 可选：起始月（两位）
END_M="${4:-12}"           # 可选：结束月（两位）

BASE="https://oceandata.sci.gsfc.nasa.gov/ob/getfile"
COOKIE="${HOME}/.urs_cookies"

if [[ "$REG" == "CONUS" ]]; then
  TAIL="ILW_CONUS.V5.all.CONUS.300m.nc"
else
  TAIL="ILW_ALASKA.V5.all.ILW_AK.300m.nc"
fi

expand_years() {
  local spec="$1"
  if [[ "$spec" =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
    seq "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
  else
    echo "$spec"
  fi
}
last_day_of_month() {
  date -d "$1-$2-01 +1 month -1 day" +%d
}

for YEAR in $(expand_years "$YEARS"); do
  OUT="Merged/${YEAR}/${REG}_MO"
  mkdir -p "$OUT"
  echo "==> Merged-S3-ILW MONTHLY  $REG  $YEAR  →  $OUT"

  for m in $(seq -w "$START_M" "$END_M"); do
    begin="${YEAR}${m}01"
    last="$(last_day_of_month "$YEAR" "$m")"
    end="${YEAR}${m}${last}"

    file="S3M_OLCI_EFRNT.${begin}_${end}.L3m.MO.${TAIL}"
    url="${BASE}/${file}"
    dest="${OUT}/${file}"

    code=$(curl -s -L -n -c "$COOKIE" -b "$COOKIE" -o /dev/null -w '%{http_code}' "$url")
    if [[ "$code" != "200" ]]; then
      echo "miss  $file   (HTTP $code)"
      continue
    fi

    if [[ -s "$dest" ]]; then
      echo "skip  $file"
    else
      echo "get   $file"
      curl -L -n -C - --retry 3 --retry-delay 2 \
           -c "$COOKIE" -b "$COOKIE" \
           -o "$dest" "$url"
    fi
  done
done

echo "Done."