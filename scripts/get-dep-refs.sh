#!/usr/bin/env bash
set -euo pipefail
get_ref() {
    local APP=$1
    #echo "{ok,Raw}=file:consult(\"rebar.lock\"), {_Vsn, Deps}=hd(Raw), {_, {git, _Url, {ref, Ref}},_} = lists:keyfind(<<\"${APP}\">>,1, Deps), io:format(\"~s\",[Ref]), init:stop()."
    erl -noshell -eval "{ok,Raw}=file:consult(\"rebar.lock\"), {_Vsn, Deps}=hd(Raw), {_, {git, _Url, {ref, Ref}},_} = lists:keyfind(<<\"${APP}\">>,1, Deps), io:format(\"~s\",[Ref]), init:stop()."
}

rebar3 get-deps
echo "::set-output name=DEP_QUICER_REF::$(get_ref quicer)"
