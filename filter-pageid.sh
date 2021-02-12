#!/usr/bin/env bash
# shellcheck disable=SC2128
SOURCED=false && [ "$0" = "$BASH_SOURCE" ] || SOURCED=true

if ! $SOURCED; then
  set -euo pipefail
  IFS=$'\n\t'
fi

#################### utils
function check_int() {
  local re='^[0-9]+$'
  local mynum="$1"
  local option="$2"

  if ! [[ "$mynum" =~ $re ]] ; then
     (echo -n "Error in option '$option': " >&2)
     (echo "must be an integer, got $mynum." >&2)
     exit 1
  fi

  if ! [ "$mynum" -ge 0 ] ; then
     (echo "Error in option '$option': must be integer, got $mynum." >&2)
     exit 1
  fi
}
#################### end: utils


SCRIPTNAME="$(basename "$0")"
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

START_ID=0
STEP=2000000
END_ID=20000000
#################### help
function short_usage() {
  (>&2 echo \
"Usage:
  $SCRIPTNAME [options] <input>... <output>
  $SCRIPTNAME -h
")
}

function usage() {
  (>&2 short_usage )
  (>&2 echo \
"
Launch filter-pageid.

Options:
  -v                Show verbose information.
  -n                Do not run any command.
  -s START_ID       Start id [default: $START_ID].
  -e END_ID         End id [default: $END_ID].
  -p STEP           Step id [default: $STEP].
  -h                Show this help and exits.

Example:
  $SCRIPTNAME"
  )
}

help_flag=false
debug=false
dry_run=false
verbose=false

# default
start_id="$START_ID"
end_id="$END_ID"
step="$STEP"

while getopts ":e:dhnp:s:v" opt; do
  case $opt in
    e)
      check_int "$OPTARG" '-e'
      end_id="$OPTARG"
      ;;
    d)
      debug=true
      ;;
    h)
      help_flag=true
      ;;
    n)
      dry_run=true
      ;;
    p)
      check_int "$OPTARG" '-p'
      step="$OPTARG"
      ;;
    s)
      check_int "$OPTARG" '-t'
      start_id="$OPTARG"
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
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
pyenv activate wikiconv 2>/dev/null
set -u

echoverbose "VIRTUAL_ENV: $VIRTUAL_ENV"

echoverbose "Options:"
echoverbose "  - start_id: $start_id"
echoverbose "  - end_id: $end_id"
echoverbose "  - step: $step:"

for id in $(seq "$start_id" "$step" "$end_id"); do
  sid="$id"
  if [ "$id" -ne "$start_id" ]; then
    sid="$((id+1))"
  fi
  if [ "$sid" -ge "$end_id" ]; then
    break;
  fi
	eid="$((id+step))"

	echoverbose "sid: $sid - eid $eid"

  if $debug; then set -x; fi
  (
    cd "$SCRIPTDIR"
    run python3 -m wikiconv-crunch --output-compression gzip \
      "${INPUT_ARR[@]}" "$OUTPUT" \
	    filter-pageid --start-id "$sid" --end-id "$eid"
  )

done

exit 0