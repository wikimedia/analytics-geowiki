#!/bin/bash
#
# This script is only the second part of the full workflow to generate
# the geowiki dashboards.
#
# The first part is shovelling data from the "recent changes" database
# into the research database. You can achieve that first part via
# ../geowiki/process_data.py
#
# On 2013-09-03, running this script took ~1 hour.
#

set -e

SCRIPT_DIR_ABS="$(dirname "$0")"
MAIN_SCRIPT_FILE_ABS="$SCRIPT_DIR_ABS/make_limn_files.py"

error() {
	echo "Error: $@" >&2
	exit 1
}

#---------------------------------------------------
# Setting up python environment.
#
# ottomata decided that it's not worth the effort to puppetize each
# and every of erosen's python repos. So in order to get the scripts
# running, we have to rely on a prepared set-up.
# That's bad.
# Like really bad.
# But at least it allows us to run the scripts for now.

PYTHON_SHIM_BASE_DIR_ABS=/home/qchris/
export PYTHONPATH="$PYTHONPATH:${PYTHON_SHIM_BASE_DIR_ABS}/wp-zero/wikimarkup-1.01b1+encoding_patch+removed_django_depends"
export PYTHONPATH="$PYTHONPATH:${PYTHON_SHIM_BASE_DIR_ABS}/wp-zero/src/limnpy"
export PYTHONPATH="$PYTHONPATH:${PYTHON_SHIM_BASE_DIR_ABS}/wp-zero/src/mcc-mnc"
export PYTHONPATH="$PYTHONPATH:${PYTHON_SHIM_BASE_DIR_ABS}/wp-zero/src/wikipandas"
export PYTHONPATH="$PYTHONPATH:${PYTHON_SHIM_BASE_DIR_ABS}/global-dev/dashboard/src/gcat"
export PYTHONPATH="$PYTHONPATH:${PYTHON_SHIM_BASE_DIR_ABS}/.local/lib/python2.7/site-packages"

#---------------------------------------------------
# Filtering parameters
CRON_MODE=no
BASE_DIR_PUBLIC_DIR_REL=
BASE_DIR_PRIVATE_DIR_REL=
for PARAM in "$@"
do
	case "$PARAM" in
		"--help" | "-h" )
			"$MAIN_SCRIPT_FILE_ABS" --help
			exit 0
			;;
		"--basedir_public="* | "-d="* )
			BASE_DIR_PUBLIC_DIR_REL="${PARAM//*=/}"
			;;
		"--basedir_private="* )
			BASE_DIR_PRIVATE_DIR_REL="${PARAM//*=/}"
			;;
	esac
done

if [ "$1" = "--cron-mode" ]
then
	# In cron mode, we respawn ourself and hold back output. We only
	# show the output upon error.
	shift
	LOG_FILE_ABS=$(mktemp --tmpdir "geowiki-log.XXXXXX")
	EXIT_CODE=1
	if "$0" "$@" &>"$LOG_FILE_ABS"
	then
		EXIT_CODE=0
	else
		cat "$LOG_FILE_ABS" >&2
	fi
	rm -f "$LOG_FILE_ABS"
	exit "$EXIT_CODE"
fi

if [ -z "$BASE_DIR_PUBLIC_DIR_REL" ]
then
	error "No --basedir_public provided."
fi

if [ -z "$BASE_DIR_PRIVATE_DIR_REL" ]
then
	error "No --basedir_private provided."
fi

#---------------------------------------------------
# Actual making and pushing of limn files

# Cleaning data repos
for REPO_DIR_REL in "$BASE_DIR_PUBLIC_DIR_REL" "$BASE_DIR_PRIVATE_DIR_REL"
do
	pushd "$REPO_DIR_REL" >/dev/null
	git reset --hard
	git pull
	popd >/dev/null
done

# Recompute limn files
"$MAIN_SCRIPT_FILE_ABS" "$@"

# Get the most recent date. We assume en_all.csv always has at least one
# editor on any given day :-)
LAST_DATE="$(tail --lines=1 "$BASE_DIR_PRIVATE_DIR_REL/datafiles/en_all.csv" | cut --field=1 --delimiter=, )"
LAST_DATE=${LAST_DATE////-}

# Commit and push limn files
for REPO_DIR_REL in "$BASE_DIR_PUBLIC_DIR_REL" "$BASE_DIR_PRIVATE_DIR_REL"
do
	pushd "$REPO_DIR_REL" >/dev/null
	git add -A
	git commit -m "Automatic commit of data up to $LAST_DATE"
	git push
	popd >/dev/null
done
