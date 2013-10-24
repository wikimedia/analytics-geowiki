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
BASE_DIR_REL=
for PARAM in "$@"
do
	case "$PARAM" in
		"--help" | "-h" )
			"$MAIN_SCRIPT_FILE_ABS" --help
			exit 0
			;;
		"--basedir="* | "-d="* )
			BASE_DIR_REL="${PARAM//*=/}"
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

if [ -z "$BASE_DIR_REL" ]
then
	error "No --basedir provided."
fi

#---------------------------------------------------
# Actual making and pushing of limn files

# Cleaning data repo
cd "$BASE_DIR_REL"
git reset --hard
git pull
cd -

# Recompute limn files
"$MAIN_SCRIPT_FILE_ABS" "$@"

# Commit and push limn files
cd "$BASE_DIR_REL"
git add -A
LAST_DATE="$(tail --lines=1 datafiles/en_all.csv | cut --field=1 --delimiter=, )"
LAST_DATE=${LAST_DATE////-}
git commit -m "Automatic commit of data up to $LAST_DATE"
git push
cd -
