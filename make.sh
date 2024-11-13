#!/bin/bash
set -eu

command="${1}"
shift

if [[ "${command}" == init ]]; then
	if [[ ! -d venv ]]; then
        python -m venv venv
   	fi 
	source venv/bin/activate
    pip install -e .
elif [[ "${command}" == server ]]; then
        python tfm/server/server.py
elif [[ "${command}" == test ]]; then
    if [[ -z "${1-}" ]]; then
        python -m unittest discover -s tests
    else
        python -m unittest discover -s tests -k "${1}"
    fi
elif [[ "${command}" == reload ]]; then
	kill -SIGHUP "$(cat /tmp/tfmpid)"
elif [[ "${command}" == clean ]]; then
    rm -r venv 
fi
