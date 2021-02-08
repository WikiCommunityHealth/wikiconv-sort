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
  -n            Do not run any command.
  -h            Show this help and exits.

Example:
  $SCRIPTNAME"
  )
}

help_flag=false
debug=false
dry_run=false
verbose=false

while getopts ":dhnv" opt; do
  case $opt in
    d)
      debug=true
      ;;
    h)
      help_flag=true
      ;;
    n)
      dry_run=true
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

if $dry_run; then
  function run() {
    true -- "$@"
  }
else
  function run() {
    "$@"
  }
fi
#################### end: help

#################### utils
if $debug; then verbose=true; fi

if $debug; then
  echodebug() {
    (>&2 echo -e "[$(date '+%F %H:%M:%S')][debug]\t" "$@" )
  }
else
  echodebug() { true; }
fi

if $verbose; then
  echoverbose() {
    (>&2 echo -e "[$(date '+%F %H:%M:%S')][info]\t" "$@" 1>&2;)
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

#   activate pyenv
set +u
source "$HOME"/.pyenv/versions/3.8.2/envs/wikiconv/bin/activate
set -u

echoverbose "VIRTUAL_ENV: $VIRTUAL_ENV"
  
START_ID=0
STEP=2000000
END_ID=$((20000000-1))

for id in $(seq "$START_ID" "$STEP" "$END_ID"); do
  sid="$id"
  if [ "$id" -ne 0 ]; then
    sid="$((id+1))"
  fi
	eid="$((id+STEP))"

	echoverbose "start_id: $sid - end_id $eid"

  if $debug; then set -x; fi
	run python3 -m wikiconv-crunch --output-compression gzip \
    "${INPUT_ARR[@]}" "$OUTPUT" \
	  filter-pageid --start-id "$sid" --end-id "$eid"

done

exit 0