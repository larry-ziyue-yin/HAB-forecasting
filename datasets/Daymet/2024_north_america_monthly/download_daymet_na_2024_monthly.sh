#!/bin/bash

GREP_OPTIONS=''

cookiejar=$(mktemp cookies.XXXXXXXXXX)
netrc=$(mktemp netrc.XXXXXXXXXX)
chmod 0600 "$cookiejar" "$netrc"
function finish {
  rm -rf "$cookiejar" "$netrc"
}

trap finish EXIT
WGETRC="$wgetrc"

prompt_credentials() {
    echo "Enter your Earthdata Login or other provider supplied credentials"
    read -p "Username (ziyue.yin): " username
    username=${username:-ziyue.yin}
    read -s -p "Password: " password
    echo "machine urs.earthdata.nasa.gov login $username password $password" >> $netrc
    echo
}

exit_with_error() {
    echo
    echo "Unable to Retrieve Data"
    echo
    echo $1
    echo
    echo "https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_pr_2024.tif"
    echo
    exit 1
}

prompt_credentials
  detect_app_approval() {
    approved=`curl -s -b "$cookiejar" -c "$cookiejar" -L --max-redirs 5 --netrc-file "$netrc" https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_pr_2024.tif -w '\n%{http_code}' | tail  -1`
    if [ "$approved" -ne "200" ] && [ "$approved" -ne "301" ] && [ "$approved" -ne "302" ]; then
        # User didn't approve the app. Direct users to approve the app in URS
        exit_with_error "Please ensure that you have authorized the remote application by visiting the link below "
    fi
}

setup_auth_curl() {
    # Firstly, check if it require URS authentication
    status=$(curl -s -z "$(date)" -w '\n%{http_code}' https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_pr_2024.tif | tail -1)
    if [[ "$status" -ne "200" && "$status" -ne "304" ]]; then
        # URS authentication is required. Now further check if the application/remote service is approved.
        detect_app_approval
    fi
}

setup_auth_wget() {
    # The safest way to auth via curl is netrc. Note: there's no checking or feedback
    # if login is unsuccessful
    touch ~/.netrc
    chmod 0600 ~/.netrc
    credentials=$(grep 'machine urs.earthdata.nasa.gov' ~/.netrc)
    if [ -z "$credentials" ]; then
        cat "$netrc" >> ~/.netrc
    fi
}

# fetch_urls() {
#   if command -v curl >/dev/null 2>&1; then
#       setup_auth_curl
#       while read -r line; do
#         # Get everything after the last '/'
#         filename="${line##*/}"

#         # Strip everything after '?'
#         stripped_query_params="${filename%%\?*}"

#         curl -f -b "$cookiejar" -c "$cookiejar" -L --netrc-file "$netrc" -g -o $stripped_query_params -- $line && echo || exit_with_error "Command failed with error. Please retrieve the data manually."
#       done;
#   elif command -v wget >/dev/null 2>&1; then
#       # We can't use wget to poke provider server to get info whether or not URS was integrated without download at least one of the files.
#       echo
#       echo "WARNING: Can't find curl, use wget instead."
#       echo "WARNING: Script may not correctly identify Earthdata Login integrations."
#       echo
#       setup_auth_wget
#       while read -r line; do
#         # Get everything after the last '/'
#         filename="${line##*/}"

#         # Strip everything after '?'
#         stripped_query_params="${filename%%\?*}"

#         wget --load-cookies "$cookiejar" --save-cookies "$cookiejar" --output-document $stripped_query_params --keep-session-cookies -- $line && echo || exit_with_error "Command failed with error. Please retrieve the data manually."
#       done;
#   else
#       exit_with_error "Error: Could not find a command-line downloader.  Please install curl or wget"
#   fi
# }
fetch_urls() {
  if command -v curl >/dev/null 2>&1; then
      setup_auth_curl
      while read -r line; do
        # Get everything after the last '/'
        filename="${line##*/}"

        # Strip everything after '?'
        stripped_query_params="${filename%%\?*}"

        # ====== 新增：如果已存在同名文件，尝试断点续传 ======
        if [ -f "$stripped_query_params" ]; then
            echo "[INFO] File exists: $stripped_query_params"
            echo "[INFO] Try resume download with curl -C - ..."
            curl -C - -f -b "$cookiejar" -c "$cookiejar" -L \
                 --netrc-file "$netrc" -g \
                 -o "$stripped_query_params" -- "$line" \
                 && echo "[OK] Resume finished: $stripped_query_params" \
                 || exit_with_error "Resume failed for $stripped_query_params. Please check or delete this file and retry."
        else
            echo "[INFO] Download new file: $stripped_query_params"
            curl -f -b "$cookiejar" -c "$cookiejar" -L \
                 --netrc-file "$netrc" -g \
                 -o "$stripped_query_params" -- "$line" \
                 && echo "[OK] Download finished: $stripped_query_params" \
                 || exit_with_error "Command failed with error. Please retrieve the data manually."
        fi
      done;
  elif command -v wget >/dev/null 2>&1; then
      echo
      echo "WARNING: Can't find curl, use wget instead."
      echo "WARNING: Script may not correctly identify Earthdata Login integrations."
      echo
      setup_auth_wget
      while read -r line; do
        filename="${line##*/}"
        stripped_query_params="${filename%%\?*}"

        # ====== wget 版本也加上断点续传 -c ======
        if [ -f "$stripped_query_params" ]; then
            echo "[INFO] File exists: $stripped_query_params"
            echo "[INFO] Try resume download with wget -c ..."
            wget -c --load-cookies "$cookiejar" --save-cookies "$cookiejar" \
                 --output-document "$stripped_query_params" \
                 --keep-session-cookies -- "$line" \
                 && echo "[OK] Resume finished: $stripped_query_params" \
                 || exit_with_error "Resume failed for $stripped_query_params. Please check or delete this file and retry."
        else
            echo "[INFO] Download new file: $stripped_query_params"
            wget --load-cookies "$cookiejar" --save-cookies "$cookiejar" \
                 --output-document "$stripped_query_params" \
                 --keep-session-cookies -- "$line" \
                 && echo "[OK] Download finished: $stripped_query_params" \
                 || exit_with_error "Command failed with error. Please retrieve the data manually."
        fi
      done;
  else
      exit_with_error "Error: Could not find a command-line downloader.  Please install curl or wget"
  fi
}

fetch_urls <<'EDSCEOF'
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_pr_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_prcp_monttl_pr_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_pr_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_prcp_monttl_hi_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_vp_monavg_pr_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmax_monavg_pr_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmax_monavg_na_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmax_monavg_na_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmin_monavg_na_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_prcp_monttl_na_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmin_monavg_na_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_prcp_monttl_hi_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmin_monavg_hi_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_vp_monavg_pr_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmax_monavg_pr_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_vp_monavg_na_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmax_monavg_hi_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_hi_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_vp_monavg_na_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_hi_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_vp_monavg_hi_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmax_monavg_hi_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_na_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmin_monavg_pr_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmin_monavg_hi_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_vp_monavg_hi_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_prcp_monttl_na_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_swe_monavg_na_2024.tif
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_tmin_monavg_pr_2024.nc
https://data.ornldaac.earthdata.nasa.gov/protected/daymet/Daymet_Monthly_V4R1/data/daymet_v4_prcp_monttl_pr_2024.nc
EDSCEOF