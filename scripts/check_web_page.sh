#!/bin/bash
#
# This script checks if the geowiki files on the wmf webpages are up
# to date and it's numbers are within reasonable ranges.
#

set -e

# Absolute path to this script
SCRIPT_DIR_ABS="$(dirname "$0")"

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
# Checks that geowiki's files are ok.
#
# Input:
#   -
#
# Output:
#   -
#
check() {
    error "not yet implemented"
}


#---------------------------------------------------

check

echo "geowiki: Ok"
