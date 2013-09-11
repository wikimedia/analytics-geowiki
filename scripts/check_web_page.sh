#!/bin/bash
#
# This script checks if the geowiki files on the wmf webpages are up
# to date and it's numbers are within reasonable ranges.
#

set -e

# Automatic cleanup of temporary file by trapping EXIT. (See
# DEBUG=local below)
TMP_FILES_ABS=()
cleanup() {
    if [ "$DEBUG" != "local" ]
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

# Urls to download files from
URL_BASE="http://gp.wmflabs.org/"
URL_BASE_DASHBOARD="$URL_BASE/dashboards"
URL_BASE_GRAPH="$URL_BASE/graphs"

# Set DEBUG to "local" to download files into /tmp and use those copies
# instead of fetching the files again and again for each run. Files do
# not get removed upon script exit. This is only useful when
# debugging/developing the script
DEBUG=
#DEBUG=local

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
# Creates a temporary file
#
# The temporary file gets added to the TMP_FILES_ABS array, and hence
# removed upon exit of the script, if not in DEBUG=local mode. (See
# trap at top of this script).
#
# Input:
#   $1 - stencil to be used for the temporary file. This stencil gets
#        prepended by "geowiki_monitor." and has a randow string
#        appended.
#
# Output:
#   TMP_FILE_ABS - The absolucte name of the created temporary file. Do
#       not clean up the file. It is removed automatically (when not
#       in DEBUG=local).
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
# Downloads a URL to $DOWNLOADED_FILE_ABS without considering caches.
#
# Rather use the download_file function instead, as do_download_file
# does not pick up previously downloaded files.
#
# This function does not respect DEBUG=local.
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
    wget -O "$DOWNLOADED_FILE_ABS" -o /dev/null "$URL"
}

#---------------------------------------------------
# Downloads a URL to $DOWNLOADED_FILE_ABS.
#
# When not in DEBUG=local mode, the file is downloaded from the given
# URL into a temporary file (which automatically gets removed upon
# script exit). The name of this temporary file is passed back.
#
# When in DEBUG=local mode, a canonical file name for the URL is
# generated under /tmp/. If the file does not exist, the URL gets
# downloaded into this URL. The canonical file name for the URL is
# returned.
#
# Input:
#   $1 - The url to download
#
# Output:
#   DOWNLOADED_FILE_ABS - The absolute name of the file into which the
#       URL's content can be found. Do not modify this file, as it may
#       be reused for different runs, when in DEBUG=local mode. Do not
#       clean up the file. It is removed automatically (when not in
#       DEBUG=local).
#
download_file() {
    local URL="$1"

    local SAFE_URL="$(echo "$URL" | sed -e 's/[^a-zA-Z0-9_.-]/_/g')"

    if [ "$DEBUG" = "local" ]
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
#       be reused for different runs, when in DEBUG=local mode. Do not
#       clean up the file. It is removed automatically (when not in
#       DEBUG=local).
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
    error "checking datasources not yet implemented"
    error "checking datafiles not yet implemented"
}


#---------------------------------------------------

check

echo "geowiki: Ok"
