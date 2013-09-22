#!/bin/bash
#
# This script checks if the geowiki files on the wmf webpages are up
# to date and it's numbers are within reasonable ranges.
#

set -e

# Automatic cleanup of temporary file by trapping EXIT. (See
# USE_CACHE variable below)
TMP_FILES_ABS=()
cleanup() {
    if [ "$USE_CACHE" != "yes" ]
    then
	for TMP_FILE_ABS in "${TMP_FILES_ABS[@]}"
	do
	    rm -f "$TMP_FILE_ABS"
	    if [ -e "$TMP_FILE_ABS" ]
	    then
		echo "Could not remove temporary file: $TMP_FILE_ABS" >&2
	    fi
	done
    fi
}
trap "cleanup" EXIT

# Absolute path to this script
SCRIPT_DIR_ABS="$(dirname "$0")"

# Verbosity levels
VERBOSITY_NORMAL=10
VERBOSITY_VERBOSE=11
VERBOSITY="$VERBOSITY_NORMAL"

# Urls to download files from
URL_BASE="http://gp.wmflabs.org/"
URL_BASE_DASHBOARD="$URL_BASE/dashboards"
URL_BASE_GRAPH="$URL_BASE/graphs"
URL_BASE_DATASOURCE="$URL_BASE/datasources"
URL_BASE_CSV="$URL_BASE/data/datafiles/gp"

# Wikis with many active editors.
# For those wikis, we use allow less deviation from linear
# extrapolated values for the column with maximum active editors.
WIKIS_MANY_ACTIVE_EDITORS=( ar az be be_x_old bg bn ca cs da de el en eo es et eu fa fi gl he hi hr hu hy id is it ja ka kk kn ko lt lv mk ml mn mr ms nl nn no pl pt ro ru sh simple sk sl sr sq sv ta te th tl tr uk vi zh zh_yue )

# Wikis with hardly any active editors.
# For those wikis, linear extrapolation does not make sense. However,
# we check whether the needed files exist.
WIKIS_HARDLY_ACTIVE_EDITORS=( ab ace af ak als am an ang arc arz as ast av ay ba bar bat_smg bcl bh bi bjn bm bo bpy br bs bug bxr cbk_zam cdo ce ceb ch chr chy ckb co cr crh csb cu cv cy diq dsb dv dz ee eml ext ff fiu_vro fj fo frp frr fur fy ga gan gd glk gn got gu gv ha hak haw hif hsb ht ia ie ig ik ilo io iu jbo jv kaa kab kbd kg ki kl km koi krc ks ksh ku kv kw ky la lad lb lbe lez lg li lij lmo ln lo ltg map_bms mdf mg mhr mi mrj mt mwl my myv mzn na nah nap nds nds_nl ne new nov nrm nso nv ny oc om or os pa pag pam pap pcd pdc pi pih pms pnb pnt ps qu rm rmy rn roa_rup roa_tara rue rw sa sah sc scn sco sd se sg si sm sn so srn ss st stq su sw szl tet tg ti tk tn to tpi ts tt tum tw ty udm ug ur uz ve vec vep vls vo wa war wo wuu xal xh yi yo za zea zh_classical zh_min_nan zu )

# Some files have a last data point that is further back. Either
# because the wiki ran out of active editors, or the file has to be
# generated by hand. The EXPECTED_LAST_DATE_OVERRIDE array is used to
# override the default expected date (i.e.: current day) of the last
# data point of a file.
declare -A EXPECTED_LAST_DATE_OVERRIDE
EXPECTED_LAST_DATE_OVERRIDE["global_south_editor_fractions"]="2013-08-20"
EXPECTED_LAST_DATE_OVERRIDE["grants_count_by_global_south"]="2013-06-01"
EXPECTED_LAST_DATE_OVERRIDE["grants_count_by_program"]="2013-06-01"
EXPECTED_LAST_DATE_OVERRIDE["grants_spending_by_global_south"]="2013-06-01"
EXPECTED_LAST_DATE_OVERRIDE["grants_spending_by_program"]="2013-06-01"
EXPECTED_LAST_DATE_OVERRIDE["lg_all"]="2013-08-09"
EXPECTED_LAST_DATE_OVERRIDE["lg_top10"]="2013-06-20"
EXPECTED_LAST_DATE_OVERRIDE["pnt_all"]="2013-08-15"
EXPECTED_LAST_DATE_OVERRIDE["pnt_top10"]="2013-08-01"
EXPECTED_LAST_DATE_OVERRIDE["rn_top10"]="2013-08-27"
EXPECTED_LAST_DATE_OVERRIDE["sg_all"]="2013-09-05"
EXPECTED_LAST_DATE_OVERRIDE["sg_top10"]="2013-09-05"
EXPECTED_LAST_DATE_OVERRIDE["sm_all"]="2013-09-07"
EXPECTED_LAST_DATE_OVERRIDE["sm_top10"]="2013-09-07"
EXPECTED_LAST_DATE_OVERRIDE["ti_all"]="2013-09-07"
EXPECTED_LAST_DATE_OVERRIDE["ti_top10"]="2013-09-07"
EXPECTED_LAST_DATE_OVERRIDE["to_top10"]="2013-09-05"
EXPECTED_LAST_DATE_OVERRIDE["ts_top10"]="2013-08-26"
EXPECTED_LAST_DATE_OVERRIDE["tum_all"]="2013-08-06"
EXPECTED_LAST_DATE_OVERRIDE["tum_top10"]="2013-08-06"
EXPECTED_LAST_DATE_OVERRIDE["ve_all"]="2013-07-31"
EXPECTED_LAST_DATE_OVERRIDE["ve_top10"]="2013-06-07"

# Set USE_CACHE to "yes" to download files into /tmp and use those
# copies instead of fetching the files again and again for each
# run. Files do not get removed upon script exit. This is only useful
# when debugging/developing the script.
#
# (You can use the --cache parameter to set USE_CACHE=yes temporarily)
USE_CACHE=no
#USE_CACHE=yes

# Add directories here that are checkous of the data repositories. It
# is tried to copy data over from them instead of downloading it from
# the web. This is mostly useful for debugging.
LOCAL_DATA_CHECKOUTS_DIR_RELI=()

#---------------------------------------------------
# Prints the script's help screen
#
# Input:
#   -
#
# Output:
#   -
#
print_help() {
    cat <<EOF
check_web_page.sh [ OPTIONS ]

checks if the data served for the geowiki repository is up-to-date and
the columns meet expectations.

OPTIONS:
--help, -h       -- prints this help page
--add-checkout DIR
                 -- before downloading files, try to find them in
                    DIR. You can pass this option multiple times.
                    This is useful for debugging, with DIR being a
                    checkout of the geowiki-data or dashboard-data
                    repository.
--cache          -- cache the downloaded documents into /tmp/geowiki_monitor...
                    and reuse them on subsequent runs. This is useful
                    for debugging the script. But you'll have to
                    cleanup the /tmp/geowiki_monitor... files by hand
                    on your own.
--verbose        -- More verbose output.

EOF
}

#---------------------------------------------------
# Parses arguments to the script
#
# Input:
#   $1, $2, ... arguments that should get processed
#
# Output:
#   USE_CACHE
#   LOCAL_DATA_CHECKOUTS_DIR_RELI
#   VERBOSITY
#
parse_arguments() {
    while [ $# -gt 0 ]
    do
	local ARGUMENT="$1"
	shift
        case "$ARGUMENT" in
            "--help" | "-h" )
                print_help
                exit 0
                ;;
            "--add-checkout" )
		[[ $# -ge 1 ]] || error "$ARGUMENT requires a further parameter"
		LOCAL_DATA_CHECKOUTS_DIR_RELI=( "${LOCAL_DATA_CHECKOUTS_DIR_RELI[@]}" "$1" )
		shift
                ;;
            "--cache" )
                USE_CACHE="yes"
                ;;
            "--verbose" )
                VERBOSITY="$VERBOSITY_VERBOSE"
                ;;
	    * )
		error "unknown argument '$ARGUMENT'"
        esac
    done
}

#---------------------------------------------------
# Dumps an error message to stderr and exits the script
#
# Concatenation of "Error:" $1, $2, ... gets used as error message.
#
# Input:
#   $1, $2, ... used as error message
#
# Output:
#   -
#
error() {
    echo "Error: $@" >&2
    exit 1
}

#---------------------------------------------------
# Logs a message if its not above the verbosity threshold
#
# Concatenation of "Error:" $1, $2, ... gets used as error message.
#
# Input:
#   $1 - Verbosity threshold
#   $2, $3, ... used as message
#
# Output:
#   -
#
log() {
    local MESSAGE_VERBOSITY="$1"
    shift
    if [ "$MESSAGE_VERBOSITY" -le "$VERBOSITY" ]
    then
	echo "$@"
    fi
}

#---------------------------------------------------
# Creates a temporary file
#
# The temporary file gets added to the TMP_FILES_ABS array, and hence
# removed upon exit of the script, if not in using caching. (See trap
# at top of this script).
#
# Input:
#   $1 - stencil to be used for the temporary file. This stencil gets
#        prepended by "geowiki_monitor." and has a randow string
#        appended.
#
# Output:
#   TMP_FILE_ABS - The absolucte name of the created temporary file. Do
#       not clean up the file. It is removed automatically when not
#       using caching.
#
mktemp_file() {
    local NAME="$1"
    TMP_FILE_ABS=$(mktemp --tmpdir geowiki_monitor.$NAME.XXXXXX)
    if [ -z "$TMP_FILE_ABS" ]
    then
	error "Could not create new temporary file"
    fi
    if [ ! -e "$TMP_FILE_ABS" ]
    then
	error "Could not create temporary file '$TMP_FILE_ABS'"
    fi
    TMP_FILES_ABS=( "${TMP_FILES_ABS[@]}" "${TMP_FILE_ABS}" )
}

#---------------------------------------------------
# Get the expected date for the last data point in a file.
#
# The date is typically the current day. However, some wikis do not
# have any active editors, so for them, the last active data point
# might be further back. Other files get generated by hand, so their
# expected last data point is from a different day as well.
#
# Input:
#   $1 - Stub of the file's name for which to set the expected date.
#        E.g.: "ee_top10", "lg_all", "global_south".
#
# Output:
#   EXPECTED_LAST_DATE - The date (in YYYY-MM-DD format) of the last
#        data point that is expected for this stub.
#
set_EXPECTED_LAST_DATE() {
    local STUB="$1"

    EXPECTED_LAST_DATE="${EXPECTED_LAST_DATE_OVERRIDE[$STUB]}"
    if [ -z "$EXPECTED_LAST_DATE" ]
    then
	EXPECTED_LAST_DATE="$(date +'%Y-%m-%d')"
    fi
}

#---------------------------------------------------
# Downloads a URL to $DOWNLOADED_FILE_ABS without considering caches.
#
# Rather use the download_file function instead, as do_download_file
# does not pick up previously downloaded files.
#
# This function overrides caching, and forces downloading the URL.
#
# Input:
#   $1 - The url to download
#
# Output:
#   DOWNLOADED_FILE_ABS - The absolute name of the file into which the
#       URL has been downloaded.
#
do_download_file() {
    local URL="$1"

    # LOCAL_COPY_FILE_RELI is either empty (if no local copy of $URL
    # has yet been found), or is holds the file name to the found
    # local copy of $URL.
    local LOCAL_COPY_FILE_RELI=

    # The file we try to find on local data checkouts.
    local NEEDLE="$URL"
    # Strip leading URL_BASE if present
    if [ "${NEEDLE:0:${#URL_BASE}}" = "${URL_BASE}" ]
    then
	NEEDLE="${NEEDLE:${#URL_BASE}}"
    fi
    # Strip data and gp from /data/.../gp/ paths. This helps finding
    # data files.
    NEEDLE="$(echo "${NEEDLE}" | sed -e 's@^/data/\(.*\)/gp/@\1/@')"

    for LOCAL_DATA_CHECKOUT_DIR_RELI in "${LOCAL_DATA_CHECKOUTS_DIR_RELI[@]}"
    do
	local CANDIDATE_FILE_RELI=

	# Check for direct match
	if [ -z "$LOCAL_COPY_FILE_RELI" ]
	then
	    local CANDIDATE_FILE_RELI="$LOCAL_DATA_CHECKOUT_DIR_RELI/$NEEDLE"
	    if [ -e "$CANDIDATE_FILE_RELI" ]
	    then
		LOCAL_COPY_FILE_RELI="$CANDIDATE_FILE_RELI"
	    fi
	fi

	# Check for json extension match. This helps to find dashboards.
	if [ -z "$LOCAL_COPY_FILE_RELI" ]
	then
	    local CANDIDATE_FILE_RELI="$LOCAL_DATA_CHECKOUT_DIR_RELI/$NEEDLE.json"
	    if [ -e "$CANDIDATE_FILE_RELI" ]
	    then
		LOCAL_COPY_FILE_RELI="$CANDIDATE_FILE_RELI"
	    fi
	fi

	# Check for handmade matches
	if [ -z "$LOCAL_COPY_FILE_RELI" ]
	then
	    local CANDIDATE_FILE_RELI="$LOCAL_DATA_CHECKOUT_DIR_RELI/$NEEDLE"
	    CANDIDATE_FILE_RELI="${CANDIDATE_FILE_RELI/graphs/graphs/handmade}"
	    if [ -e "$CANDIDATE_FILE_RELI" ]
	    then
		LOCAL_COPY_FILE_RELI="$CANDIDATE_FILE_RELI"
	    fi
	fi
    done

    if [ ! -z "$LOCAL_COPY_FILE_RELI" -a -e "$LOCAL_COPY_FILE_RELI" ]
    then
	cp "$LOCAL_COPY_FILE_RELI" "$DOWNLOADED_FILE_ABS"
    else
	log "$VERBOSITY_VERBOSE" "Downloading $URL ..."
	wget -O "$DOWNLOADED_FILE_ABS" -o /dev/null "$URL"
    fi
}

#---------------------------------------------------
# Downloads a URL to $DOWNLOADED_FILE_ABS.
#
# When not using caching, the file is downloaded from the given URL
# into a temporary file (which automatically gets removed upon script
# exit). The name of this temporary file is passed back.
#
# When using caching, a canonical file name for the URL is generated
# under /tmp/. If the file does not exist, the URL gets downloaded
# into this URL. The canonical file name for the URL is returned.
#
# Input:
#   $1 - The url to download
#
# Output:
#   DOWNLOADED_FILE_ABS - The absolute name of the file into which the
#       URL's content can be found. Do not modify this file, as it may
#       be reused for different runs, when using caching. Do not clean
#       up the file. It is removed automatically when not using
#       caching.
#
download_file() {
    local URL="$1"

    local SAFE_URL="$(echo "$URL" | sed -e 's/[^a-zA-Z0-9_.-]/_/g')"

    if [ "$USE_CACHE" = "yes" ]
    then
	DOWNLOADED_FILE_ABS=/tmp/geowiki_monitor."$SAFE_URL"
	if [ ! -e "$DOWNLOADED_FILE_ABS" ]
	then
	    do_download_file "$URL"
	fi
    else
	local TMP_FILE_ABS
	mktemp_file "$SAFE_URL"
	DOWNLOADED_FILE_ABS="$TMP_FILE_ABS"
	do_download_file "$URL"
    fi
}

#---------------------------------------------------
# Downloads a csv file and does some basic checks on the file
#
# It is assured that
#   * the file's last line is for the expected date,
#   * each line of the file has the same amount of columns, and
#   * (for stubs != "global_south_editor_fractions") that the file has
#     at least 10 columns.
#
# Input:
#   $1 - The csv's stub to download. The URL to download is generated
#        from this stub. E.g.: 'global_south', 'de_top10'.
#
# Output:
#   DOWNLOADED_FILE_ABS - The absolute name of the file into which the
#       URL's content can be found. Do not modify this file, as it may
#       be reused for different runs, when using caching. Do not clean
#       up the file. It is removed automatically when not using
#       caching.
#
check_csv() {
    local CSV_STUB="$1"
    download_file "${URL_BASE_CSV}/${CSV_STUB}.csv"

    # We'll analyze the last line for date and number of columns
    local LAST_LINE="$(tail --lines=1 "$DOWNLOADED_FILE_ABS")"

    local DATE_LAST_LINE=${LAST_LINE%%,*}
    DATE_LAST_LINE=${DATE_LAST_LINE////-}

    local EXPECTED_LAST_DATE=
    set_EXPECTED_LAST_DATE "$CSV_STUB"

    # Check for csv's date in the last line
    if [ "$EXPECTED_LAST_DATE" != "${DATE_LAST_LINE}" ]
    then
	error "$CSV_STUB's last line is not for $EXPECTED_LAST_DATE, but '$DATE_LAST_LINE'"
    fi

    # Check that the csv has at least 10 columns
    if [ "$CSV_STUB" != "global_south_editor_fractions" ]
    then
	if [ "$(tr ',' '\n' <<<"$LAST_LINE" | wc -l)" -lt 10 ]
	then
	    error "$CSV_STUB's last line has less than 10 columns"
	fi
    fi

    # Check that all lines have the same number of columns
    #
    # We use greediness of sed REs to de-escape the csv's first line,
    # and strip everything but , and \n using tr.
    if [ "$(sed -e '1s/"[^"]*"//g' "$DOWNLOADED_FILE_ABS" | tr --complement --delete ",\n" | sort -u | wc -l)" != "1" ] # '
    then
	error "$CSV_STUB has different number of columns for different rows"
    fi
}

#---------------------------------------------------
# Downloads a json file and does some basic checks on the file
#
# It is assured (not by parsing the json, but by some basic grepping)
#   * the file contains a matching "id" node, and
#   * the file contains a name node that is a string.
#
# Input:
#   $1 - The kind of file. I.e.: either "graph", or "datasource".
#   $2 - The file's stub. E.g.: "active_editors_by_region",
#        "pt_top10".
#   $3 - The url to download (without the ".json" ending)
#
# Output:
#   DOWNLOADED_FILE_ABS - The absolute name of the file into which the
#       URL's content can be found. Do not modify this file, as it may
#       be reused for different runs, when using caching. Do not clean
#       up the file. It is removed automatically when not using
#       caching.
#
check_json() {
    local KIND="$1"
    local JSON_STUB="$2"
    local URL_WITHOUT_EXTENSION="$3"
    download_file "${URL_WITHOUT_EXTENSION}.json"

    # We do not parse the json, just to some basic sanity checks.

    # Check for matching id
    if ! grep -q '"id"[[:space:]]*:[[:space:]]*"'"$JSON_STUB"'"' "${DOWNLOADED_FILE_ABS}"
    then
	error "Could not find id '$JSON_STUB' for $KIND '$JSON_STUB'"
    fi

    # Check for name being a string
    if ! grep -q '"name"[[:space:]]*:[[:space:]]*"' "${DOWNLOADED_FILE_ABS}"
    then
	error "Could not find 'name' node of type string for $KIND '$JSON_STUB'"
    fi
}

#---------------------------------------------------
# Asserts that a datapoint is within expectations.
#
# The expectations are:
#    * The dates for the three data points differ by a fixed number of
#      days (See $2).
#    * The final data point's value is within an interval extrapolated
#      from the previous two readings (See $3).
#    * The final data point's value is within an absolute interval
#      (See $4, $5).
#
# Input:
#   $1 - The expected date (in YYYY-MM-DD format) for the last data
#        point. E.g: "2013-08-01"
#   $2 - The number of days between the data rows. E.g.: 7
#   $3 - The allowed percentual deviation (in percent) of the last
#        data point from the linear extrapolation of the previous two
#        rows. E.g.: 10
#        With the exemplary data from the stdin description below,
#        linear extrapolation would expect the reading 1300. Using
#        $3 = 10, all readings in [1170,1430] for the final row would
#        be considered within expectations.
#   $4 - (optional) Absolute lower bound for the final reading to be
#        considered within expectations. E.g.: 1250
#   $5 - (optional) Absolute upper bound for the final reading to be
#        considered within expectations. E.g.: 1500
#   $6 - (optional) Affix appended to error messages.
#   stdin - Expects three lines, each of the format "DATE VALUE",
#       where DATE is (in YYYY-MM-DD format), and VALUE is an integer
#       number. E.g.:
#            2013-07-18 1100
#            2013-07-25 1200
#            2013-08-01 1310
#       The above exemplary values, this stdin represents readings
#       within expectations.
#
check_csv_column_unstrided() {
    local EXPECTED_LAST_DATE="$1"
    local STRIDE="$2"
    local EXTRAPOLATION_ERROR_PERCENTUAL="$3"
    local ABSOLUTE_LOWER="$4"
    local ABSOLUTE_UPPER="$5"
    local ERROR_AFFIX="$6"

    # First data point for linear interpolation
    local DATE1=
    local TREND_START=0
    read DATE1 TREND_START
    if [ "$DATE1" != "$(date -d "$EXPECTED_LAST_DATE - $((STRIDE*2)) days" +'%Y-%m-%d' )" ]
    then
	error "Date of trend start did not match expected value for $ERROR_AFFIX"
    fi

    # Second data point for linear interpolation
    local DATE2=
    local TREND_END=0
    read DATE2 TREND_END
    if [ "$DATE2" != "$(date -d "$EXPECTED_LAST_DATE - $((STRIDE)) days" +'%Y-%m-%d' )" ]
    then
	error "Date of trend end did not match expected value for $ERROR_AFFIX"
    fi

    # Final data point
    local DATE3=
    local VALUE=0
    read DATE3 VALUE
    if [ "$DATE3" != "$(date -d "$EXPECTED_LAST_DATE" +'%Y-%m-%d' )" ]
    then
	error "Date of trend end did not match expected value for $ERROR_AFFIX"
    fi

    # Checking if data point is within extrapolated interval
    local LINEAR_EXTRAPOLATION=$((2*$TREND_END - $TREND_START))
    local LINEAR_EXTRAPOLATION_LOWER="$(((LINEAR_EXTRAPOLATION*((100-EXTRAPOLATION_ERROR_PERCENTUAL)))/100))"
    local LINEAR_EXTRAPOLATION_UPPER="$(((LINEAR_EXTRAPOLATION*((100+EXTRAPOLATION_ERROR_PERCENTUAL)))/100))"
    if [ "$VALUE" -lt "$LINEAR_EXTRAPOLATION_LOWER" -o "$LINEAR_EXTRAPOLATION_UPPER" -lt "$VALUE" ]
    then
	error "Value $VALUE ($DATE3) not in extrapolation interval [$LINEAR_EXTRAPOLATION_LOWER,$LINEAR_EXTRAPOLATION_UPPER] for $ERROR_AFFIX"
    fi

    # Checking if data point is within absolute interval
    if [ ! -z "$ABSOLUTE_LOWER" ]
    then
	if [ "$VALUE" -lt "$ABSOLUTE_LOWER" ]
	then
	    error "Value $VALUE ($DATE3) below lower absolute threshold $ABSOLUTE_LOWER for $ERROR_AFFIX"
	fi
    fi
    if [ ! -z "$ABSOLUTE_UPPER" ]
    then
	if [ "$VALUE" -gt "$ABSOLUTE_UPPER" ]
	then
	    error "Value $VALUE ($DATE3) above upper absolute threshold $ABSOLUTE_UPPER for $ERROR_AFFIX"
	fi
    fi
}

#---------------------------------------------------
# Extracts data from a csv file and checks data for three data points.
#
# Input:
#   $1 - Stub of the csv's file name.
#   $2 - Absolute file name of the csv
#   $3 - The 1-based index of the column to check
#   $4 - The name of the column to check (only used for error messages)
#   $5 - Stride to extract data for. E.g.: 1 takes the last three
#        lines. 7 takes rows n-14, n-7, and n (with n being the number
#        of rows in the file).
#   $6 - The allowed percentual deviation (in percent) of the last
#        data point from the linear extrapolation of the previous two
#        rows. (See $3 of check_csv_column_unstrided)
#   $7 - (optional) Absolute lower bound for the final reading to be
#        considered within expectations. E.g.: 1300
#   $8 - (optional) Absolute upper bound for the final reading to be
#        considered within expectations. E.g.: 1500
#
# Output:
#   -
#
check_csv_column_fixed_stride() {
    local CSV_STUB="$1"
    local CSV_FILE_ABS="$2"
    local COLUMN="$3"
    local COLUMN_NAME="$4"
    local STRIDE="$5"
    local EXTRAPOLATION_ERROR_PERCENTUAL="$6"
    local ABSOLUTE_LOWER="$7"
    local ABSOLUTE_UPPER="$8"

    local EXPECTED_LAST_DATE=
    set_EXPECTED_LAST_DATE "$CSV_STUB"

    tail --lines="$((1+2*$STRIDE))" <"$CSV_FILE_ABS" | \
	cut -f "1,$COLUMN" -d ',' | \
	sed -n -e '1~'"$STRIDE"'{s@\([0-9]\{4\}\)/\([0-9]\{2\}\)/\([0-9]\{2\}\),[[:space:]]*\([0-9]\+\)\(\.[0-9]*\)\?$@\1-\2-\3 \4@;p}' | \
        check_csv_column_unstrided "$EXPECTED_LAST_DATE" "$STRIDE" "$EXTRAPOLATION_ERROR_PERCENTUAL" "$ABSOLUTE_LOWER" "$ABSOLUTE_UPPER" "column '$COLUMN_NAME' of $CSV_STUB (stride: $STRIDE)"
}

#---------------------------------------------------
# Obtains the column number for a column name for csv files.
#
# Input:
#   $1 - The name or 1-based index of the column to find.
#
# Output:
#   COLUMN_IDX - The 1-based index for the column
#
set_COLUMN_IDX_csv() {
    local CSV_STUB="$1"
    local CSV_FILE_ABS="$2"
    local COLUMN="$3"
    if [[ "$COLUMN" =~ ^[[:digit:]]+$ ]]
    then
	COLUMN_IDX="$COLUMN"
    else
	COLUMN_IDX=
	local COUNTER=1
	local ACCUMULATOR=
	while read CAPTION
	do
	    if [ ! -z "$ACCUMULATOR" ]
	    then
		CAPTION="$ACCUMULATOR,$CAPTION"
	    fi

	    if [ "$(($(echo "$CAPTION" | tr '"' '\n' | wc -l) % 2))" = "1" ] #'
	    then
		if [ "$CAPTION" = "$COLUMN" ]
		then
		    COLUMN_IDX="$COUNTER"
		fi
		COUNTER=$((COUNTER+1))
		ACCUMULATOR=
	    else
		ACCUMULATOR="$CAPTION"
	    fi
	done < <( head --lines="1" <"$CSV_FILE_ABS" | tr ',' '\n' )
	if [ -z "$COLUMN_IDX" ]
	then
	    error "Could not find column '$COLUMN' in $CSV_STUB"
	fi
    fi
}

#---------------------------------------------------
# Checks the expectations of the recent data for a single column
#
# Checks that:
#   * The final reading matches extrapolated data from the previous
#     two days.
#   * The final reading matches extrapolated data from one and two
#     weeks back.
#
# Input:
#   $1 - Stub of the csv's file name.
#   $2 - Absolute file name of the csv
#   $3 - The name or 1-based index of the column to find.
#   $4 - The allowed percentual deviation (in percent) of the last
#        data point from the linear extrapolation of the previous two
#        days. (See $3 of check_csv_column_unstrided)
#   $5 - The allowed percentual deviation (in percent) of the last
#        data point from the linear extrapolation of the previous two
#        weeks. (See $3 of check_csv_column_unstrided)
#   $6 - (optional) Absolute lower bound for the final reading to be
#        considered within expectations. E.g.: 1300
#   $7 - (optional) Absolute upper bound for the final reading to be
#        considered within expectations. E.g.: 1500
#
# Output:
#   -
#
check_csv_column() {
    local CSV_STUB="$1"
    local CSV_FILE_ABS="$2"
    local COLUMN="$3"
    local EXTRAPOLATION_ERROR_PERCENTUAL_1="$4"
    local EXTRAPOLATION_ERROR_PERCENTUAL_7="$5"
    local ABSOLUTE_LOWER="$6"
    local ABSOLUTE_UPPER="$7"

    local COLUMN_IDX=0
    set_COLUMN_IDX_csv "$CSV_STUB" "$CSV_FILE_ABS" "$COLUMN"

    check_csv_column_fixed_stride "$CSV_STUB" "$CSV_FILE_ABS" "$COLUMN_IDX" "$COLUMN" 7 "$EXTRAPOLATION_ERROR_PERCENTUAL_7" "$ABSOLUTE_LOWER"  "$ABSOLUTE_UPPER"
    check_csv_column_fixed_stride "$CSV_STUB" "$CSV_FILE_ABS" "$COLUMN_IDX" "$COLUMN" 1 "$EXTRAPOLATION_ERROR_PERCENTUAL_1"
}

#---------------------------------------------------
# Asserts that the global_south csv is ok
#
# Input:
#   $1 - Stub of the csv's file name.
#
# Output:
#   -
#
check_csv_global_south() {
    local CSV_STUB="global_south"

    local DOWNLOADED_FILE_ABS=
    check_csv "$CSV_STUB"
    local CSV_FILE_ABS="$DOWNLOADED_FILE_ABS"

    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Global North (100+)" 2  3   7000   9000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Global North (5+)"   2  2  55000  65000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Global North (all)"  2  2 170000 210000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Global South (100+)" 3  5   1400   1800
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Global South (5+)"   2  3  13000  16000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Global South (all)"  2  3  40000  60000
    # In the following three lines, the missing n in 'Unkown' is on
    # purpose, as we currently see that in the csv.
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Unkown (100+)"      25 60     10     40
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Unkown (5+)"        10 20    200    300
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Unkown (all)"        7 20    700   1000
}

#---------------------------------------------------
# Asserts that the global_south_editor_fractions csv is ok
#
# Input:
#   $1 - Stub of the csv's file name.
#
# Output:
#   -
#
check_csv_global_south_editor_fractions() {
    local CSV_STUB="global_south_editor_fractions"

    local DOWNLOADED_FILE_ABS=
    check_csv "$CSV_STUB"

    local TMP_FILE_ABS=
    mktemp_file "${CSV_STUB}_rescaled"
    local CSV_RESCALED_FILE_ABS="$TMP_FILE_ABS"
    # Poor man's rescaling for floating points >0, <1, and having at least
    # 5 fractional digits.
    sed -e 's/,0.\([0-9]\{5\}\)[0-9]*/,x\1/g' -e 's/,x0*/,/g' "$DOWNLOADED_FILE_ABS" >"$CSV_RESCALED_FILE_ABS"

    check_csv_column "$CSV_STUB" "$CSV_RESCALED_FILE_ABS" "Global South Fraction (100+)" 2  5 15000 19000
    check_csv_column "$CSV_STUB" "$CSV_RESCALED_FILE_ABS" "Global South Fraction (5+)"   1  2 17000 21000
    check_csv_column "$CSV_STUB" "$CSV_RESCALED_FILE_ABS" "Global South Fraction (all)"  1  2 18000 22000
}

#---------------------------------------------------
# Asserts that the region csv is ok
#
# Input:
#   $1 - Stub of the csv's file name.
#
# Output:
#   -
#
check_csv_region() {
    local CSV_STUB="region"

    local DOWNLOADED_FILE_ABS=
    check_csv "$CSV_STUB"
    local CSV_FILE_ABS="$DOWNLOADED_FILE_ABS"

    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Africa (100+)"             14  30     20     40
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Africa (5+)"               10  15    250    450
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Africa (all)"               3   8   1150   1600
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Arab States (100+)"         8  10     70    180
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Arab States (5+)"           3   6   1000   1300
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Arab States (all)"          2   5   3500   5000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Asia & Pacific (100+)"      2   5   1300   1700
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Asia & Pacific (5+)"        1   2  14700  15600
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Asia & Pacific (all)"       1   2  45000  51000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "CIS (100+)"                 4   9    200    400
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "CIS (5+)"                   2   5   1500   2200
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "CIS (all)"                  2   3   4200   5800
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Europe (100+)"              1   3   5000   6000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Europe (5+)"                2   2  35000  40000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Europe (all)"               1   2 110000 120000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "North America (100+)"       2   5   1400   1800
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "North America (5+)"         2   4  14000  16100
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "North America (all)"        2   3  50000  60000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "South/Latin America (100+)" 4   6    400    600
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "South/Latin America (5+)"   2   4   4000   6000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "South/Latin America (all)"  2   4  16000  20000
    # In the following three lines, the missing n in 'Unkown' is on
    # purpose, as we currently see that in the csv.
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Unkown (100+)"             50 120      2     15
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Unkown (5+)"               12  40     80    130
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Unkown (all)"              14  28    200    450
}

#---------------------------------------------------
# Checks that the top10 data for ptwiki (Brazil!) is ok
#
# Input:
#   $1 - Stub of the csv's file name.
#
# Output:
#   -
#
check_csv_pt_top10() {
    local CSV_STUB="pt_top10"

    local DOWNLOADED_FILE_ABS=
    check_csv "$CSV_STUB"
    local CSV_FILE_ABS="$DOWNLOADED_FILE_ABS"

    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Brazil (100+)"        8 15  100  200
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Brazil (5+)"          2  7  800 1400
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Brazil (all)"         2  4 3000 4000
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "France (all)"        15 25   35   80
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Germany (all)"       15 25   60  110
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Portugal (all)"       6 14  300  550
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "Spain (all)"         15 40   20   70
    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "United States (all)" 13 30   60  130
}

#---------------------------------------------------
# Finds the maximum of a csv row that is 30 rows old.
#
# This function obtains the maximum value (and the corresponding
# column index) for the last but 30 row of a csv file.
#
# This function is usefull, when trying to analyze generic 'per wiki'
# files. There, this function is used to find the column that had most
# active editors some time back, and can therefore be expected to be
# among the columns with most editors for the csv most recent row.
#
# Input:
#   $1 - Absolute file name of the csv to find maximum for
#
# Output:
#   COLUMN_WITH_MAXIMUM - the 1-based index of the column containing a
#                         maximum (0 if no maximum was found).
#   MAXIMUM             - the maximum (0 if no maximum was found)
#
find_maximum() {
    local CSV_FILE_ABS="$1"

    local LINE="$(tail -n 31  "$CSV_FILE_ABS" | head -n 1)"
    LINE=${LINE#*,}

    COLUMN_WITH_MAXIMUM=0
    MAXIMUM=0

    local NIBBLE
    local COLUMN=2
    while [ ! -z "$LINE" ]
    do
	NIBBLE=${LINE%%,*}
	NIBBLE=${NIBBLE%%.*}
	if [ ! -z "$NIBBLE" ]
	then
	    if [ "$NIBBLE" -gt "$MAXIMUM" ]
	    then
		MAXIMUM="$NIBBLE"
		COLUMN_WITH_MAXIMUM="$COLUMN"
	    fi
	fi

	OLD_LINE="$LINE"
	LINE=${LINE#*,}
	if [ "$LINE" = "$OLD_LINE" ]
	then
	    LINE=
	else
	    COLUMN=$((COLUMN+1))
	fi
    done
}

#---------------------------------------------------
# Checks expectations for a csv of a wiki with many active editors.
#
# It is assured that
#   * The maximum active editors count from 31 rows back is larger
#     than 30.
#   * The column of the maximum active editors from 31 rows back has
#     still more at least 20 active editors, and did not change "too
#     much" recently.
#
# Input:
#   $1 - Stub of the csv's file name. E.g.: "de_all", "eo_top10"
#
# Output:
#   -
#
check_csv_wiki_many_active_editors() {
    local CSV_STUB="$1"

    local DOWNLOADED_FILE_ABS=
    check_csv "$CSV_STUB"
    local CSV_FILE_ABS="$DOWNLOADED_FILE_ABS"

    local MAXIMUM=
    local COLUMN_WITH_MAXIMUM=

    #find_maximum sets MAXIMUM and COLUMN_WITH_MAXIMUM
    find_maximum "$CSV_FILE_ABS"

    if [ "$MAXIMUM" -lt "30" ]
    then
	if [ "$MAXIMUM" -gt "0" ]
	then
	    error "Maximum ($MAXIMUM) is low for $CSV_STUB. Please reclassify as wiki with hardly editors"
	else
	    error "No maximum column found for $CSV_STUB"
	fi
    fi

    check_csv_column "$CSV_STUB" "$CSV_FILE_ABS" "$COLUMN_WITH_MAXIMUM" 20 60 20
}

#---------------------------------------------------
# Checks expectations for a csv of a wiki with hardly any active editors.
#
# It is assured that
#   * If there was a maximum active editors count from 31 rows back,
#     it is larger now below.
#
# Input:
#   $1 - Stub of the csv's file name. E.g.: "ab_all", "li_top10"
#
# Output:
#   -
#
check_csv_wiki_hardly_active_editors() {
    local CSV_STUB="$1"

    local DOWNLOADED_FILE_ABS=
    check_csv "$CSV_STUB"
    local CSV_FILE_ABS="$DOWNLOADED_FILE_ABS"

    local MAXIMUM=
    local COLUMN_WITH_MAXIMUM=

    #find_maximum sets MAXIMUM and COLUMN_WITH_MAXIMUM
    find_maximum "$CSV_FILE_ABS"

    if [ "$COLUMN_WITH_MAXIMUM" != "0" ]
    then
	VALUE="$(tail --lines=1 "$CSV_FILE_ABS" | cut -f "$COLUMN_WITH_MAXIMUM" -d ",")"
	VALUE="${VALUE%%.*}"
	if [ ! -z "$VALUE" ]
	then
	    if [ "$VALUE" -gt 50 ]
	    then
		error "Maximum ($VALUE) is high for $CSV_STUB. Please reclassify as wiki with many editors"
	    fi
	fi
    fi
}

#---------------------------------------------------
# Checks generic expectations for all wikis' all and top10 csvs.
#
# Input:
#   -
#
# Output:
#   -
#
check_csv_wikis() {
    local WIKI
    local CSV_VARIANT
    for CSV_VARIANT in all top10
    do
	for WIKI in "${WIKIS_MANY_ACTIVE_EDITORS[@]}"
	do
	    check_csv_wiki_many_active_editors "${WIKI}_${CSV_VARIANT}"
	done

	for WIKI in "${WIKIS_HARDLY_ACTIVE_EDITORS[@]}"
	do
	    check_csv_wiki_hardly_active_editors "${WIKI}_${CSV_VARIANT}"
	done
    done
}

#---------------------------------------------------
# Downloads a dashboard file and does some basic checks on the file
#
# It is assured that
#   * the file exists and is not empty
#
# Input:
#   $1 - The dashboards's stub to download. The URL to download is generated
#        from this stub. E.g.: 'reportcard'.
#
# Output:
#   -
#
check_dashboard() {
    local DASHBOARD_STUB="$1"

    local DOWNLOADED_FILE_ABS=
    download_file "${URL_BASE_DASHBOARD}/${DASHBOARD_STUB}"

    # The downloaded file only contains the code to load limn, so
    # there is not too much we can check for. But at least the server
    # should respond with a non-empty page
    if [ ! -s "${DOWNLOADED_FILE_ABS}" ]
    then
	error "No content for URL dashboard $DASHBOARD_STUB"
    fi
}

#---------------------------------------------------
# Downloads a graph file and does some basic checks on the file
#
# It is assured (not by parsing the json, but by some basic grepping)
#   * the file contains a matching "id" node, and
#   * the file contains a name node that is a string.
#
# Input:
#   $2 - The file's stub. E.g.: "active_editors_by_region",
#        "pt_top10".
#
# Output:
#   -
#
check_graph() {
    local GRAPH_STUB="$1"

    local DOWNLOADED_FILE_ABS=
    check_json "graph" "${GRAPH_STUB}" "${URL_BASE_GRAPH}/${GRAPH_STUB}"

    # Check for matching slug
    if ! grep -q '"slug"[[:space:]]*:[[:space:]]*"'"$GRAPH_STUB"'"' "${DOWNLOADED_FILE_ABS}"
    then
	error "Could not find slug '$GRAPH_STUB' for graph '$GRAPH_STUB'"
    fi
}

#---------------------------------------------------
# Downloads a datasource file and does some basic checks on the file
#
# It is assured (not by parsing the json, but by some basic grepping)
#   * the file contains a matching "id" node, and
#   * the file contains a name node that is a string.
#
# Input:
#   $1 - The datasource's stub. E.g.: "active_editors_by_region",
#        "pt_top10".
#   $2 - The datafile's format. I.e.: "json" or "csv"
#
# Output:
#   -
#
check_datasource() {
    local DATASOURCE_STUB="$1"
    local DATAFILE_FORMAT="$2"

    local DOWNLOADED_FILE_ABS=
    check_json "datasource" "${DATASOURCE_STUB}" "${URL_BASE_DATASOURCE}/${DATASOURCE_STUB}"


    # We do not parse the json, just to some further datasource specific checks.

    # check for format
    if ! grep -q '"format"[[:space:]]*:[[:space:]]*"'"$DATAFILE_FORMAT"'"' "${DOWNLOADED_FILE_ABS}"
    then
	error "Could not find marker for format '$DATAFILE_FORMAT' for datasource '$DATASOURCE_STUB'"
    fi

    # check for URL
    # EXPECTED_URL is used as RE.
    local EXPECTED_URL=
    if [ "$DATASOURCE_STUB" = "map-world_countries" ]
    then
	EXPECTED_URL="/data/geo/gp/maps/world-countries.json"
    else
	EXPECTED_URL="/data/datafiles/gp/$DATASOURCE_STUB.$DATAFILE_FORMAT"
    fi
    if ! grep -q '"url"[[:space:]]*:[[:space:]]*"'"$EXPECTED_URL"'"' "${DOWNLOADED_FILE_ABS}"
    then
	error "Could not URL of the form '$EXPECTED_URL' for datasource '$DATASOURCE_STUB'"
    fi

    # Format specific checks
    case "$DATAFILE_FORMAT" in
	"csv" )
	    # Check for end date
	    set_EXPECTED_LAST_DATE "$DATASOURCE_STUB"
	    if ! grep -q '"end"[[:space:]]*:[[:space:]]*"'"${EXPECTED_LAST_DATE//-//}"'"' "${DOWNLOADED_FILE_ABS}"
	    then
		error "Could not find end date ${EXPECTED_LAST_DATE} for datasource '$DATASOURCE_STUB'"
	    fi
	    ;;
    esac
}

#---------------------------------------------------
# Checks that geowiki's dashboards are ok.
#
# Input:
#   -
#
# Output:
#   -
#
check_dashboards() {
    check_dashboard reportcard
}

#---------------------------------------------------
# Checks that geowiki's graphs are ok.
#
# Input:
#   -
#
# Output:
#   -
#
check_graphs() {
    check_graph active_editors_by_region
    check_graph global_north_south_active_editors
    check_graph global_south_editor_fractions
    check_graph grants_count_by_global_south
    check_graph grants_count_by_program
    check_graph grants_spending_by_country
    check_graph grants_spending_by_global_south
    check_graph grants_spending_by_program
    check_graph pt_wp_brazil
}

#---------------------------------------------------
# Checks that geowiki's datasources are ok.
#
# Input:
#   -
#
# Output:
#   -
#
check_datasources() {
    check_datasource global_south        csv
    check_datasource map-world_countries json
    check_datasource pt_top10            csv
    check_datasource region              csv

    # The following datasources somewhat belong to geowiki, but are
    # not part of the geowiki, so they have to be generated by
    # hand. We monitor them to avoid data loss, but geowiki does not
    # allow to keep the up-to-date. So we cannot check that they are
    # up-to-date.
    check_datasource global_south_editor_fractions   csv
    check_datasource grants_count_by_global_south    csv
    check_datasource grants_count_by_program         csv
    check_datasource grants_spending_by_country      json
    check_datasource grants_spending_by_global_south csv
    check_datasource grants_spending_by_program      csv
}

#---------------------------------------------------
# Checks that geowiki's datafiles are ok.
#
# Input:
#   -
#
# Output:
#   -
#
check_datafiles() {
    check_csv_global_south
    check_csv_global_south_editor_fractions
    check_csv_region
    check_csv_pt_top10
    check_csv_wikis
}

#---------------------------------------------------
# Checks that geowiki's files are ok.
#
# Input:
#   -
#
# Output:
#   -
#
check() {
    check_dashboards
    check_graphs
    check_datasources
    check_datafiles
}


#---------------------------------------------------

parse_arguments "$@"
check

echo "geowiki: Ok"
