#!/usr/bin/env bash
set -euo pipefail

# 用法：
#   bash download_ilw_monthly.sh 2024 S3B CONUS
#   bash download_ilw_monthly.sh 2020-2024 S3A ALASKA        # 连续年份区间
#   bash download_ilw_monthly.sh 2024 S3A CONUS 03 10        # 只下 3-10 月
#
# 依赖：
#   1) Linux GNU date（HPC常见）
#   2) 已在 ~/.netrc 写好 Earthdata 账号（含 urs 与 oceandata 两条），权限 600
#   3) 首次用浏览器对 oceandata 站点点过 Authorize（或第一次命令会自动写入 cookie）

YEARS="$1"                  # e.g., "2024" 或 "2020-2024"
SAT="${2:-S3A}"             # S3A | S3B
REG="${3:-CONUS}"           # CONUS | ALASKA
START_M="${4:-01}"          # 可选：起始月（两位）
END_M="${5:-12}"            # 可选：结束月（两位）

# 目录与文件尾缀
if [[ "$REG" == "CONUS" ]]; then
  TAIL="ILW_CONUS.V5.all.CONUS.300m.nc"
else
  TAIL="ILW_ALASKA.V5.all.ILW_AK.300m.nc"
fi

BASE="https://oceandata.sci.gsfc.nasa.gov/ob/getfile"
COOKIE="$HOME/.urs_cookies"

# 将年份参数展开为列表
expand_years() {
  local spec="$1"
  if [[ "$spec" =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
    local a="${BASH_REMATCH[1]}" b="${BASH_REMATCH[2]}"
    seq "$a" "$b"
  else
    echo "$spec"
  fi
}

# 计算每月最后一天（支持闰年）
last_day_of_month() {
  local y="$1" m="$2"
  date -d "$y-$m-01 +1 month -1 day" +%d
}

for YEAR in $(expand_years "$YEARS"); do
  OUT="${SAT}/${YEAR}/${REG}_MO"
  mkdir -p "$OUT"
  echo "==> Downloading $SAT $REG $YEAR (Monthly) → $OUT"

  for m in $(seq -w "$START_M" "$END_M"); do
    begin="${YEAR}${m}01"
    last="$(last_day_of_month "$YEAR" "$m")"
    end="${YEAR}${m}${last}"

    file="${SAT}_OLCI_EFRNT.${begin}_${end}.L3m.MO.${TAIL}"
    url="${BASE}/${file}"
    dest="${OUT}/${file}"

    if [[ -s "$dest" ]]; then
      echo "skip  $file"
      continue
    fi

    # 先 HEAD 探测，下游 200 再拉取，可过滤不存在月份
    code=$(curl -s -L -n -c "$COOKIE" -b "$COOKIE" -o /dev/null -w '%{http_code}' "$url")
    if [[ "$code" != "200" ]]; then
      echo "miss  $file   (HTTP $code)"
      continue
    fi

    echo "get   $file"
    # -L 跟随跳转；-n 使用 ~/.netrc；-C - 断点续传；--retry 容错
    curl -L -n -C - --retry 3 --retry-delay 2 \
         -c "$COOKIE" -b "$COOKIE" \
         -o "$dest" "$url"
  done
done

echo "Done."