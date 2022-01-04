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

-export([run/1, run/0]).

-include_lib("emqx/include/logger.hrl").

%% `emqx_bpapi:call' enriched with dialyzer spec
-type typed_call() :: {emqx_bpapi:call(), _DialyzerSpec}.
-type typed_rpc() :: {typed_call(), typed_call()}.

-type fulldump() :: #{emqx_bpapi:api() =>
                          #{emqx_bpapi:api_version() =>
                                #{ calls := [typed_rpc()]
                                 , casts := [typed_rpc()]
                                 }
                           }}.

%% Applications we wish to ignore in the analysis:
-define(IGNORED_APPS, "gen_rpc, recon, observer_cli, snabbkaffe, ekka, mria").
%% List of known RPC backend modules:
-define(RPC_MODULES, "gen_rpc, erpc, rpc, emqx_rpc").
%% List of functions in the RPC backend modules that we can ignore:
-define(IGNORED_RPC_CALLS, "gen_rpc:nodes/0").

-define(XREF, myxref).

run() ->
    case {filelib:wildcard("*_plt"), filelib:wildcard("_build/emqx*/lib")} of
        {[PLT|_], [RelDir|_]} ->
            run(#{plt => PLT, reldir => RelDir});
        _ ->
            error("failed to guess run options")
    end.

-spec run(map()) -> boolean().
run(Opts) ->
    put(bpapi_ok, true),
    PLT = prepare(Opts),
    %% First we run XREF to find all callers of any known RPC backend:
    Callers = find_remote_calls(Opts),
    {BPAPICalls, NonBPAPICalls} = lists:partition(fun is_bpapi_call/1, Callers),
    warn_nonbpapi_rpcs(NonBPAPICalls),
    CombinedAPI = collect_bpapis(BPAPICalls, PLT),
    dump_api(CombinedAPI),
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
dump_api(Term) ->
    Filename = filename:join(code:priv_dir(emqx_bpapi), emqx_app:get_release() ++ ".bpapi"),
    file:write_file(Filename, io_lib:format("~0p.", [Term])).

-spec collect_bpapis([mfa()], _PLT) -> fulldump().
collect_bpapis(L, PLT) ->
    Modules = lists:usort([M || {M, _F, _A} <- L]),
    lists:foldl(fun(Mod, Acc) ->
                        #{ api     := API
                         , version := Vsn
                         , calls   := Calls0
                         , casts   := Casts0
                         } = Mod:bpapi_meta(),
                        Calls = enrich(PLT, Calls0),
                        Casts = enrich(PLT, Casts0),
                        Acc#{API => #{Vsn => #{ calls => Calls
                                              , casts => Casts
                                              }}}
                end,
                #{},
                Modules
               ).

%% Add information about types from the PLT
-spec enrich(_PLT, [emqx_bpapi:rpc()]) -> [typed_rpc()].
enrich(PLT, Calls) ->
    [case {lookup_type(PLT, From), lookup_type(PLT, To)} of
         {{value, TFrom}, {value, TTo}} ->
             {{From, TFrom}, {To, TTo}};
         {_, none} ->
             setnok(),
             ?CRITICAL("Backplane API function ~s calls a missing remote function ~s",
                       [format_call(From), format_call(To)]),
             error(missing_target)
     end
     || {From, To} <- Calls].

lookup_type(PLT, {M, F, A}) ->
    dialyzer_plt:lookup(PLT, {M, F, length(A)}).

format_call({M, F, A}) ->
    io_lib:format("~p:~p/~p", [M, F, length(A)]).

setnok() ->
    put(bpapi_ok, false).
