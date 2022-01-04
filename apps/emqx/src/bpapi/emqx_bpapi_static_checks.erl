%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([dump/1, dump/0]).

-include_lib("emqx/include/logger.hrl").

-type api_dump() :: #{{emqx_bpapi:api(), emqx_bpapi:api_version()} =>
                          #{ calls := [emqx_bpapi:rpc()]
                           , casts := [emqx_bpapi:rpc()]
                           }}.

-type dialyzer_spec() :: {_Type, [_Type]}.

-type dialyzer_dump() :: #{mfa() => dialyzer_spec()}.

-type fulldump() :: #{ api        => api_dump()
                     , signatures => dialyzer_dump()
                     }.

%% Applications we wish to ignore in the analysis:
-define(IGNORED_APPS, "gen_rpc, recon, observer_cli, snabbkaffe, ekka, mria").
%% List of known RPC backend modules:
-define(RPC_MODULES, "gen_rpc, erpc, rpc, emqx_rpc").
%% List of functions in the RPC backend modules that we can ignore:
-define(IGNORED_RPC_CALLS, "gen_rpc:nodes/0").

-define(XREF, myxref).

dump() ->
    case {filelib:wildcard("*_plt"), filelib:wildcard("_build/emqx*/lib")} of
        {[PLT|_], [RelDir|_]} ->
            dump(#{plt => PLT, reldir => RelDir});
        _ ->
            error("failed to guess run options")
    end.

%% Collect the local BPAPI modules to a dump file:
-spec dump(map()) -> boolean().
dump(Opts) ->
    put(bpapi_ok, true),
    PLT = prepare(Opts),
    %% First we run XREF to find all callers of any known RPC backend:
    Callers = find_remote_calls(Opts),
    {BPAPICalls, NonBPAPICalls} = lists:partition(fun is_bpapi_call/1, Callers),
    warn_nonbpapi_rpcs(NonBPAPICalls),
    APIDump = collect_bpapis(BPAPICalls),
    DialyzerDump = collect_signatures(PLT, APIDump),
    dump_api(#{api => APIDump, signatures => DialyzerDump}),
    erase(bpapi_ok).

prepare(#{reldir := RelDir, plt := PLT}) ->
    ?INFO("Starting xref...", []),
    xref:start(?XREF),
    filelib:wildcard(RelDir ++ "/*/ebin/") =:= [] andalso
        error("No applications found in the release directory. Wrong directory?"),
    xref:set_default(?XREF, [{warnings, false}]),
    xref:add_release(?XREF, RelDir),
    %% Now to the dialyzer stuff:
    ?INFO("Loading PLT...", []),
    dialyzer_plt:from_file(PLT).

find_remote_calls(_Opts) ->
    Query = "XC | (A - [" ?IGNORED_APPS "]:App)
               || ([" ?RPC_MODULES "] : Mod - " ?IGNORED_RPC_CALLS ")",
    {ok, Calls} = xref:q(?XREF, Query),
    ?INFO("Calls to RPC modules ~p", [Calls]),
    {Callers, _Callees} = lists:unzip(Calls),
    Callers.

-spec warn_nonbpapi_rpcs([mfa()]) -> ok.
warn_nonbpapi_rpcs([]) ->
    ok;
warn_nonbpapi_rpcs(L) ->
    setnok(),
    lists:foreach(fun({M, F, A}) ->
                          ?ERROR("~p:~p/~p does a remote call outside of a dedicated "
                                 "backplane API module. "
                                 "It may break during rolling cluster upgrade",
                                 [M, F, A])
                  end,
                  L).

-spec is_bpapi_call(mfa()) -> boolean().
is_bpapi_call({Module, _Function, _Arity}) ->
    case catch Module:bpapi_meta() of
        #{api := _} -> true;
        _           -> false
    end.

-spec dump_api(fulldump()) -> ok.
dump_api(Term = #{api := _, signatures := _}) ->
    Filename = filename:join(code:priv_dir(emqx), emqx_app:get_release() ++ ".bpapi"),
    file:write_file(Filename, io_lib:format("~0p.", [Term])).

-spec collect_bpapis([mfa()]) -> api_dump().
collect_bpapis(L) ->
    Modules = lists:usort([M || {M, _F, _A} <- L]),
    lists:foldl(fun(Mod, Acc) ->
                        #{ api     := API
                         , version := Vsn
                         , calls   := Calls
                         , casts   := Casts
                         } = Mod:bpapi_meta(),
                        Acc#{{API, Vsn} => #{ calls => Calls
                                            , casts => Casts
                                            }}
                end,
                #{},
                Modules
               ).

-spec collect_signatures(_PLT, api_dump()) -> dialyzer_dump().
collect_signatures(PLT, APIs) ->
    maps:fold(fun(_APIAndVersion, #{calls := Calls, casts := Casts}, Acc0) ->
                      Acc1 = lists:foldl(fun enrich/2, {Acc0, PLT}, Calls),
                      {Acc, PLT} = lists:foldl(fun enrich/2, Acc1, Casts),
                      Acc
              end,
              #{},
              APIs).

%% Add information about the call types from the PLT
-spec enrich(emqx_bpapi:rpc(), {dialyzer_dump(), _PLT}) -> {dialyzer_dump(), _PLT}.
enrich({From0, To0}, {Acc0, PLT}) ->
    From = call_to_mfa(From0),
    To   = call_to_mfa(To0),
    case {dialyzer_plt:lookup(PLT, From), dialyzer_plt:lookup(PLT, To)} of
        {{value, TFrom}, {value, TTo}} ->
            Acc = Acc0#{ From => TFrom
                       , To   => TTo
                       },
            {Acc, PLT};
        {_, none} ->
            setnok(),
            ?CRITICAL("Backplane API function ~s calls a missing remote function ~s",
                      [format_call(From0), format_call(To0)]),
            error(missing_target)
     end.

-spec call_to_mfa(emqx_bpapi:call()) -> mfa().
call_to_mfa({M, F, A}) ->
    {M, F, length(A)}.

format_call({M, F, A}) ->
    io_lib:format("~p:~p/~p", [M, F, length(A)]).

setnok() ->
    put(bpapi_ok, false).
