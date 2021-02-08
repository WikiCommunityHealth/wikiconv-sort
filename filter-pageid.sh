#!/usr/bin/env bash
# shellcheck disable=SC2128
SOURCED=false && [ "$0" = "$BASH_SOURCE" ] || SOURCED=true

if ! $SOURCED; then
  set -euo pipefail
  IFS=$'\n\t'
fi

SCRIPTNAME="$(basename "$0")"
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
#################### help
function short_usage() {
  (>&2 echo \
"Usage:
  $SCRIPTNAME [options]
  $SCRIPTNAME -h
")
}

function usage() {
  (>&2 short_usage )
  (>&2 echo \
"
Launch filter-pageid.

Options:
  -v            Show verbose information.
  -h            Show this help and exits.

Example:
  $SCRIPTNAME"
  )
}

help_flag=false
verbose=false

while getopts ":hv" opt; do
  case $opt in
    h)
      help_flag=true
      ;;
    v)
      verbose=true
      ;;
    \?)
      (>&2 echo "Error. Invalid option: -$OPTARG")
      exit 1
      ;;
    :)
      (>&2 echo "Error.Option -$OPTARG requires an argument.")
      exit 1
      ;;
  esac
done

if $help_flag; then
  usage
  exit 0
fi
#################### end: help

#################### utils
if $verbose; then
  echoverbose() { 
    echo -en "[$(date '+%F %H:%M:%S')][verbose]\t";
    echo "$@" 1>&2;
  }
else
  echoverbose() { true; }
fi
#################### end: utils

# Shell Script: is mixing getopts with positional parameters possible?
# https://stackoverflow.com/q/11742996/2377454
NARGS="$#"
INPUT="${@:$OPTIND:$((NARGS-OPTIND))}"
OUTPUT="${@:$NARGS}"

# parameter not specified
if (( NARGS-OPTIND < 1 )) ; then
  (>&2 echo "Error: parameters <input> and <output> are required.")
  short_usage
  exit 1
fi

declare -a INPUT_ARR
IFS=' ' read -a INPUT_ARR <<< "${INPUT[@]}"

echoverbose "NARGS: $NARGS"
echoverbose "OPTIND: $OPTIND"
echoverbose "INPUT_ARR:"
for arr in "${INPUT_ARR[@]}"; do
   echoverbose "  - $arr"
done
echoverbose "OUTPUT: $OUTPUT"

# activate pyenv
eval "$(pyenv init -)"
pyenv activate wikiconv 2>/dev/null

echoverbose "VIRTUAL_ENV: $VIRTUAL_ENV"

START_ID=0
STEP=2000000
END_ID=$((20000000-1))

for id in $(seq "$START_ID" "$STEP" "$END_ID"); do
	start_id="$id"
	end_id="$((id+STEP))"

	echoverbose "start_id: $start_id - end_id $end_id"
	python3 -m wikiconv-crunch --output-compression gzip \
    "${INPUT_ARR[@]}" "$OUTPUT" \
	  filter-pageid --start-id "$start_id" --end-id "$end_id"

done

exit 0