%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_security_profile_monitor).

-moduledoc """
Raises the `security_profile_divergence` alarm when nodes of one cluster run
different security profiles.

On every timer tick, the node resolves the security profile of every running
peer in the cluster, so a peer that restarts with another profile is picked up
by the next tick.

A peer is resolved through its BPAPI announcement: a peer that announces
`emqx_security_profile` support is asked for its profile over RPC, and a peer
whose announcement lacks it runs a release without security profiles, which
behaves as `legacy`. A peer that has not announced anything yet is still
booting and stays `unknown` until a later tick, as does a peer whose RPC
fails.

The process only runs on nodes with the `hardened` profile; `init/1` returns
`ignore` on `legacy` nodes. Hardened nodes name the running peers that run the
`legacy` profile. The alarm clears on the node once no running peer runs
`legacy`.
""".

-behaviour(gen_server).

-include("logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-ifdef(TEST).
-export([classify_peers/2]).
-endif.

-define(ALARM, security_profile_divergence).
-define(BPAPI_NAME, emqx_security_profile).

-define(DEFAULT_CHECK_INTERVAL, 30_000).
-define(FIRST_CHECK_DELAY, 1_000).
-define(RPC_TIMEOUT, 5_000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    case emqx_security_profile:profile() of
        hardened ->
            ok = schedule_check(?FIRST_CHECK_DELAY),
            {ok, #{}};
        legacy ->
            ignore
    end.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(check, State) ->
    ok = check(),
    ok = schedule_check(check_interval()),
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

schedule_check(Delay) ->
    _ = erlang:send_after(Delay, self(), check),
    ok.

%% The application environment key exists for tests.
check_interval() ->
    application:get_env(emqx, security_profile_check_interval, ?DEFAULT_CHECK_INTERVAL).

check() ->
    Peers = lists:delete(node(), emqx:running_nodes()),
    Classified = classify_peers(Peers, fetch_profiles(Peers)),
    ?tp(security_profile_evaluated, Classified),
    case Classified of
        #{legacy_nodes := [_ | _]} ->
            ensure_alarm_activated(Classified);
        _ ->
            emqx_alarm:ensure_deactivated(?ALARM)
    end,
    ok.

%% Peers that are not resolved are left out and count as unknown.
-spec fetch_profiles([node()]) -> #{node() => emqx_security_profile:profile()}.
fetch_profiles([]) ->
    #{};
fetch_profiles(Nodes) ->
    {Supported, Rest} = lists:partition(fun supports_profile_rpc/1, Nodes),
    %% A peer that has announced its BPAPIs without `emqx_security_profile`
    %% runs a release without security profiles: it behaves as `legacy`.
    %% A peer that has not announced anything yet is still booting.
    OldRelease = [Node || Node <- Rest, emqx_bpapi:supported_apis(Node) =/= []],
    maps:merge(
        maps:from_list([{Node, legacy} || Node <- OldRelease]),
        rpc_profiles(Supported)
    ).

supports_profile_rpc(Node) ->
    is_integer(emqx_bpapi:supported_version(Node, ?BPAPI_NAME)).

rpc_profiles([]) ->
    #{};
rpc_profiles(Nodes) ->
    Results = emqx_security_profile_proto_v1:get_profile(Nodes, ?RPC_TIMEOUT),
    maps:from_list([
        {Node, Profile}
     || {Node, {ok, Profile}} <- lists:zip(Nodes, Results),
        Profile =:= legacy orelse Profile =:= hardened
    ]).

-spec classify_peers([node()], #{node() => emqx_security_profile:profile()}) ->
    #{legacy_nodes := [node()], hardened_nodes := [node()], unknown_nodes := [node()]}.
classify_peers(Peers, Profiles) ->
    Classified = lists:foldl(
        fun(Peer, Acc) ->
            Key = classify_key(maps:get(Peer, Profiles, unknown)),
            maps:update_with(Key, fun(Nodes) -> [Peer | Nodes] end, Acc)
        end,
        #{legacy_nodes => [], hardened_nodes => [], unknown_nodes => []},
        Peers
    ),
    maps:map(fun(_Key, Nodes) -> lists:sort(Nodes) end, Classified).

classify_key(legacy) -> legacy_nodes;
classify_key(hardened) -> hardened_nodes;
classify_key(unknown) -> unknown_nodes.

%% The alarm is raised once and stays active while any running peer runs
%% `legacy`. While active, the details track the current node lists; the
%% message keeps the node list from activation time.
ensure_alarm_activated(#{legacy_nodes := Legacy} = Classified) ->
    Details = Classified#{local_profile => hardened},
    case emqx_alarm:read_details(?ALARM) of
        {error, not_found} ->
            _ = emqx_alarm:activate(?ALARM, Details, message(Legacy)),
            ok;
        {ok, Details} ->
            ok;
        {ok, _Stale} ->
            _ = emqx_alarm:update_details(?ALARM, Details),
            ok
    end.

message(Legacy) ->
    Nodes = lists:join(", ", [atom_to_list(N) || N <- Legacy]),
    iolist_to_binary([
        "Cluster nodes run different security profiles. ",
        "This node runs 'hardened'; nodes running 'legacy': ",
        Nodes,
        ". A client may be accepted on one node and rejected on another. ",
        "This is expected during a rolling upgrade until every node restarts with ",
        "the same EMQX_SECURITY_PROFILE. ",
        "The alarm details list the current legacy nodes."
    ]).
