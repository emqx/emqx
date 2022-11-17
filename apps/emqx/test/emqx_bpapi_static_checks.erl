%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bpapi_static_checks).

-export([run/0, dump/1, dump/0, check_compat/1, versions_file/0, dumps_dir/0]).

%% Using an undocumented API here :(
-include_lib("dialyzer/src/dialyzer.hrl").

-type api_dump() :: #{
    {emqx_bpapi:api(), emqx_bpapi:api_version()} =>
        #{
            calls := [emqx_bpapi:rpc()],
            casts := [emqx_bpapi:rpc()]
        }
}.

-type dialyzer_spec() :: {_Type, [_Type]}.

-type dialyzer_dump() :: #{mfa() => dialyzer_spec()}.

-type fulldump() :: #{
    api => api_dump(),
    signatures => dialyzer_dump(),
    release => string()
}.

-type dump_options() :: #{
    reldir := file:name(),
    plt := file:name()
}.

-type param_types() :: #{emqx_bpapi:var_name() => _Type}.

%% Applications and modules we wish to ignore in the analysis:
-define(IGNORED_APPS, "gen_rpc, recon, redbug, observer_cli, snabbkaffe, ekka, mria").
-define(IGNORED_MODULES, "emqx_rpc").
%% List of known RPC backend modules:
-define(RPC_MODULES, "gen_rpc, erpc, rpc, emqx_rpc").
%% List of known functions also known to do RPC:
-define(RPC_FUNCTIONS,
    "emqx_cluster_rpc:multicall/3, emqx_cluster_rpc:multicall/5"
).
%% List of functions in the RPC backend modules that we can ignore:

% TODO: handle pmap
-define(IGNORED_RPC_CALLS, "gen_rpc:nodes/0, emqx_rpc:unwrap_erpc/1").
%% List of business-layer functions that are exempt from the checks:
%% erlfmt-ignore
-define(EXEMPTIONS,
    % Reason: legacy code. A fun and a QC query are
    % passed in the args, it's futile to try to statically
    % check it
    "emqx_mgmt_api:do_query/2, emqx_mgmt_api:collect_total_from_tail_nodes/3"
).

-define(XREF, myxref).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions related to BPAPI compatibility checking
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec run() -> boolean().
run() ->
    case dump() of
        true ->
            Dumps = filelib:wildcard(dumps_dir() ++ "/*.bpapi"),
            case Dumps of
                [] ->
                    logger:error("No BPAPI dumps are found in ~s, abort", [dumps_dir()]),
                    false;
                _ ->
                    logger:notice("Running API compatibility checks for ~p", [Dumps]),
                    check_compat(Dumps)
            end;
        false ->
            logger:critical("Backplane API violations found on the current branch.", []),
            false
    end.

-spec check_compat([file:filename()]) -> boolean().
check_compat(DumpFilenames) ->
    put(bpapi_ok, true),
    Dumps = lists:map(
        fun(FN) ->
            {ok, [Dump]} = file:consult(FN),
            Dump
        end,
        DumpFilenames
    ),
    [check_compat(I, J) || I <- Dumps, J <- Dumps],
    erase(bpapi_ok).

%% Note: sets nok flag
-spec check_compat(fulldump(), fulldump()) -> ok.
check_compat(Dump1 = #{release := Rel1}, Dump2 = #{release := Rel2}) ->
    check_api_immutability(Dump1, Dump2),
    Rel2 >= Rel1 andalso
        typecheck_apis(Dump1, Dump2).

%% It's not allowed to change BPAPI modules. Check that no changes
%% have been made. (sets nok flag)
-spec check_api_immutability(fulldump(), fulldump()) -> ok.
check_api_immutability(#{release := Rel1, api := APIs1}, #{release := Rel2, api := APIs2}) when
    Rel2 >= Rel1
->
    %% TODO: Handle API deprecation
    _ = maps:map(
        fun(Key = {API, Version}, Val) ->
            case maps:get(Key, APIs2, undefined) of
                Val ->
                    ok;
                undefined ->
                    setnok(),
                    logger:error(
                        "API ~p v~p was removed in release ~p without being deprecated.",
                        [API, Version, Rel2]
                    );
                _Val ->
                    setnok(),
                    logger:error(
                        "API ~p v~p was changed between ~p and ~p. Backplane API should be immutable.",
                        [API, Version, Rel1, Rel2]
                    )
            end
        end,
        APIs1
    ),
    ok;
check_api_immutability(_, _) ->
    ok.

%% Note: sets nok flag
-spec typecheck_apis(fulldump(), fulldump()) -> ok.
typecheck_apis(
    #{release := CallerRelease, api := CallerAPIs, signatures := CallerSigs},
    #{release := CalleeRelease, signatures := CalleeSigs}
) ->
    AllCalls = lists:flatten([
        [Calls, Casts]
     || #{calls := Calls, casts := Casts} <- maps:values(CallerAPIs)
    ]),
    lists:foreach(
        fun({From, To}) ->
            Caller = get_param_types(CallerSigs, From),
            Callee = get_param_types(CalleeSigs, To),
            %% TODO: check return types
            case typecheck_rpc(Caller, Callee) of
                [] ->
                    ok;
                TypeErrors ->
                    setnok(),
                    [
                        logger:error(
                            "Incompatible RPC call: "
                            "type of the parameter ~p of RPC call ~s in release ~p "
                            "is not a subtype of the target function ~s in release ~p.~n"
                            "Caller type: ~s~nCallee type: ~s~n",
                            [
                                Var,
                                format_call(From),
                                CallerRelease,
                                format_call(To),
                                CalleeRelease,
                                erl_types:t_to_string(CallerType),
                                erl_types:t_to_string(CalleeType)
                            ]
                        )
                     || {Var, CallerType, CalleeType} <- TypeErrors
                    ]
            end
        end,
        AllCalls
    ).

-spec typecheck_rpc(param_types(), param_types()) -> [{emqx_bpapi:var_name(), _Type, _Type}].
typecheck_rpc(Caller, Callee) ->
    maps:fold(
        fun(Var, CalleeType, Acc) ->
            #{Var := CallerType} = Caller,
            case erl_types:t_is_subtype(CallerType, CalleeType) of
                true -> Acc;
                false -> [{Var, CallerType, CalleeType} | Acc]
            end
        end,
        [],
        Callee
    ).

-spec get_param_types(dialyzer_dump(), emqx_bpapi:call()) -> param_types().
get_param_types(Signatures, {M, F, A}) ->
    Arity = length(A),
    case Signatures of
        #{{M, F, Arity} := {_RetType, AttrTypes}} ->
            % assert
            Arity = length(AttrTypes),
            maps:from_list(lists:zip(A, AttrTypes));
        _ ->
            logger:critical("Call ~p:~p/~p is not found in PLT~n", [M, F, Arity]),
            error(badkey)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions related to BPAPI dumping
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump() ->
    case
        {
            filelib:wildcard(project_root_dir() ++ "/*_plt"),
            filelib:wildcard(project_root_dir() ++ "/_build/check/lib")
        }
    of
        {[PLT | _], [RelDir | _]} ->
            dump(#{
                plt => PLT,
                reldir => RelDir
            });
        _ ->
            error("failed to guess run options")
    end.

%% Collect the local BPAPI modules to a dump file
-spec dump(dump_options()) -> boolean().
dump(Opts) ->
    put(bpapi_ok, true),
    PLT = prepare(Opts),
    %% First we run XREF to find all callers of any known RPC backend:
    Callers = find_remote_calls(Opts),
    {BPAPICalls, NonBPAPICalls} = lists:partition(fun is_bpapi_call/1, Callers),
    warn_nonbpapi_rpcs(NonBPAPICalls),
    APIDump = collect_bpapis(BPAPICalls),
    DialyzerDump = collect_signatures(PLT, APIDump),
    dump_api(#{api => APIDump, signatures => DialyzerDump, release => "master"}),
    dump_versions(APIDump),
    xref:stop(?XREF),
    erase(bpapi_ok).

prepare(#{reldir := RelDir, plt := PLT}) ->
    logger:info("Starting xref...", []),
    xref:start(?XREF),
    filelib:wildcard(RelDir ++ "/*/ebin/") =:= [] andalso
        error("No applications found in the release directory. Wrong directory?"),
    xref:set_default(?XREF, [{warnings, false}]),
    xref:add_release(?XREF, RelDir),
    %% Now to the dialyzer stuff:
    logger:info("Loading PLT...", []),
    dialyzer_plt:from_file(PLT).

%% erlfmt-ignore
find_remote_calls(_Opts) ->
    Query =
        "XC | (A - ["?IGNORED_APPS"]:App - ["?IGNORED_MODULES"]:Mod - ["?EXEMPTIONS"])
           || ((["?RPC_MODULES"] : Mod + ["?RPC_FUNCTIONS"]) - ["?IGNORED_RPC_CALLS"])",
    {ok, Calls} = xref:q(?XREF, Query),
    logger:info("Calls to RPC modules ~p", [Calls]),
    {Callers, _Callees} = lists:unzip(Calls),
    Callers.

-spec warn_nonbpapi_rpcs([mfa()]) -> ok.
warn_nonbpapi_rpcs([]) ->
    ok;
warn_nonbpapi_rpcs(L) ->
    setnok(),
    lists:foreach(
        fun({M, F, A}) ->
            logger:error(
                "~p:~p/~p does a remote call outside of a dedicated "
                "backplane API module. "
                "It may break during rolling cluster upgrade",
                [M, F, A]
            )
        end,
        L
    ).

-spec is_bpapi_call(mfa()) -> boolean().
is_bpapi_call({Module, _Function, _Arity}) ->
    case catch Module:bpapi_meta() of
        #{api := _} -> true;
        _ -> false
    end.

-spec dump_api(fulldump()) -> ok.
dump_api(Term = #{api := _, signatures := _, release := Release}) ->
    Filename = filename:join(dumps_dir(), Release ++ ".bpapi"),
    ok = filelib:ensure_dir(Filename),
    file:write_file(Filename, io_lib:format("~0p.~n", [Term])).

-spec dump_versions(api_dump()) -> ok.
dump_versions(APIs) ->
    Filename = versions_file(),
    logger:notice("Dumping API versions to ~p", [Filename]),
    ok = filelib:ensure_dir(Filename),
    {ok, FD} = file:open(Filename, [write]),
    io:format(
        FD, "%% This file is automatically generated by `make static_checks`, do not edit.~n", []
    ),
    lists:foreach(
        fun(API) ->
            ok = io:format(FD, "~p.~n", [API])
        end,
        lists:sort(maps:keys(APIs))
    ),
    file:close(FD).

-spec collect_bpapis([mfa()]) -> api_dump().
collect_bpapis(L) ->
    Modules = lists:usort([M || {M, _F, _A} <- L]),
    lists:foldl(
        fun(Mod, Acc) ->
            #{
                api := API,
                version := Vsn,
                calls := Calls,
                casts := Casts
            } = Mod:bpapi_meta(),
            Acc#{
                {API, Vsn} => #{
                    calls => Calls,
                    casts => Casts
                }
            }
        end,
        #{},
        Modules
    ).

-spec collect_signatures(_PLT, api_dump()) -> dialyzer_dump().
collect_signatures(PLT, APIs) ->
    maps:fold(
        fun(_APIAndVersion, #{calls := Calls, casts := Casts}, Acc0) ->
            Acc1 = lists:foldl(fun enrich/2, {Acc0, PLT}, Calls),
            {Acc, PLT} = lists:foldl(fun enrich/2, Acc1, Casts),
            Acc
        end,
        #{},
        APIs
    ).

%% Add information about the call types from the PLT
-spec enrich(emqx_bpapi:rpc(), {dialyzer_dump(), _PLT}) -> {dialyzer_dump(), _PLT}.
enrich({From0, To0}, {Acc0, PLT}) ->
    From = call_to_mfa(From0),
    To = call_to_mfa(To0),
    case {dialyzer_plt:lookup_contract(PLT, From), dialyzer_plt:lookup(PLT, To)} of
        {{value, #contract{args = FromArgs}}, {value, TTo}} ->
            %% TODO: Check return type
            FromRet = erl_types:t_any(),
            Acc = Acc0#{
                From => {FromRet, FromArgs},
                To => TTo
            },
            {Acc, PLT};
        {{value, _}, none} ->
            setnok(),
            logger:critical(
                "Backplane API function ~s calls a missing remote function ~s",
                [format_call(From0), format_call(To0)]
            ),
            error(missing_target)
    end.

-spec call_to_mfa(emqx_bpapi:call()) -> mfa().
call_to_mfa({M, F, A}) ->
    {M, F, length(A)}.

format_call({M, F, A}) ->
    io_lib:format("~p:~p/~p", [M, F, length(A)]).

setnok() ->
    put(bpapi_ok, false).

dumps_dir() ->
    filename:join(project_root_dir(), "apps/emqx/test/emqx_static_checks_data").

project_root_dir() ->
    string:trim(os:cmd("git rev-parse --show-toplevel")).

versions_file() ->
    filename:join(project_root_dir(), "apps/emqx/priv/bpapi.versions").
