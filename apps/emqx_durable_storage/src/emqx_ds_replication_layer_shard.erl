%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_replication_layer_shard).

%% API:
-export([start_link/3]).

%% Static server configuration
-export([
    shard_servers/2,
    shard_server/3,
    local_server/2
]).

%% Dynamic server location API
-export([
    servers/3,
    server/3
]).

%% Membership
-export([
    add_local_server/2,
    drop_local_server/2,
    remove_server/3,
    server_info/2
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-type server() :: ra:server_id().

-define(MEMBERSHIP_CHANGE_TIMEOUT, 30_000).

%%

start_link(DB, Shard, Opts) ->
    gen_server:start_link(?MODULE, {DB, Shard, Opts}, []).

-spec shard_servers(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> [server()].
shard_servers(DB, Shard) ->
    ReplicaSet = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
    [shard_server(DB, Shard, Site) || Site <- ReplicaSet].

-spec shard_server(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_replication_layer_meta:site()
) -> server().
shard_server(DB, Shard, Site) ->
    {server_name(DB, Shard, Site), emqx_ds_replication_layer_meta:node(Site)}.

-spec local_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> server().
local_server(DB, Shard) ->
    {server_name(DB, Shard, local_site()), node()}.

cluster_name(DB, Shard) ->
    iolist_to_binary(io_lib:format("~s_~s", [DB, Shard])).

server_name(DB, Shard, Site) ->
    DBBin = atom_to_binary(DB),
    binary_to_atom(<<"ds_", DBBin/binary, Shard/binary, "_", Site/binary>>).

%%

-spec servers(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), Order) -> [server(), ...] when
    Order :: leader_preferred | undefined.
servers(DB, Shard, _Order = leader_preferred) ->
    get_servers_leader_preferred(DB, Shard);
servers(DB, Shard, _Order = undefined) ->
    get_shard_servers(DB, Shard).

server(DB, Shard, _Which = local_preferred) ->
    get_server_local_preferred(DB, Shard).

get_servers_leader_preferred(DB, Shard) ->
    %% NOTE: Contact last known leader first, then rest of shard servers.
    ClusterName = get_cluster_name(DB, Shard),
    case ra_leaderboard:lookup_leader(ClusterName) of
        Leader when Leader /= undefined ->
            Servers = ra_leaderboard:lookup_members(ClusterName),
            [Leader | lists:delete(Leader, Servers)];
        undefined ->
            get_online_servers(DB, Shard)
    end.

get_server_local_preferred(DB, Shard) ->
    %% NOTE: Contact either local server or a random replica.
    ClusterName = get_cluster_name(DB, Shard),
    case ra_leaderboard:lookup_members(ClusterName) of
        Servers when is_list(Servers) ->
            pick_local(Servers);
        undefined ->
            %% TODO
            %% Leader is unkonwn if there are no servers of this group on the
            %% local node. We want to pick a replica in that case as well.
            pick_random(get_online_servers(DB, Shard))
    end.

lookup_leader(DB, Shard) ->
    %% NOTE
    %% Does not block, but the result may be outdated or even unknown when there's
    %% no servers on the local node.
    ClusterName = get_cluster_name(DB, Shard),
    ra_leaderboard:lookup_leader(ClusterName).

get_online_servers(DB, Shard) ->
    filter_online(get_shard_servers(DB, Shard)).

filter_online(Servers) ->
    case lists:filter(fun is_server_online/1, Servers) of
        [] ->
            %% NOTE: Must return non-empty list.
            Servers;
        Online ->
            Online
    end.

is_server_online({_Name, Node}) ->
    Node == node() orelse lists:member(Node, nodes()).

pick_local(Servers) ->
    case lists:keyfind(node(), 2, Servers) of
        Local when is_tuple(Local) ->
            Local;
        false ->
            pick_random(Servers)
    end.

pick_random(Servers) ->
    lists:nth(rand:uniform(length(Servers)), Servers).

get_cluster_name(DB, Shard) ->
    memoize(fun cluster_name/2, [DB, Shard]).

get_local_server(DB, Shard) ->
    memoize(fun local_server/2, [DB, Shard]).

get_shard_servers(DB, Shard) ->
    maps:get(servers, emqx_ds_replication_shard_allocator:shard_meta(DB, Shard)).

local_site() ->
    emqx_ds_replication_layer_meta:this_site().

%%

%% @doc Add a local server to the shard cluster.
%% It's recommended to have the local server running before calling this function.
%% This function is idempotent.
-spec add_local_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    ok | emqx_ds:error(_Reason).
add_local_server(DB, Shard) ->
    %% NOTE
    %% Adding local server as "promotable" member to the cluster, which means
    %% that it will affect quorum until it is promoted to a voter, which in
    %% turn happens when the server has caught up sufficiently with the log.
    %% We also rely on this "membership" to understand when the server's
    %% readiness.
    ShardServers = shard_servers(DB, Shard),
    LocalServer = local_server(DB, Shard),
    case server_info(uid, LocalServer) of
        UID when is_binary(UID) ->
            ServerRecord = #{
                id => LocalServer,
                membership => promotable,
                uid => UID
            };
        unknown ->
            ServerRecord = #{
                id => LocalServer,
                membership => voter
            }
    end,
    Timeout = ?MEMBERSHIP_CHANGE_TIMEOUT,
    case ra_try_servers(ShardServers, fun ra:add_member/3, [ServerRecord, Timeout]) of
        {ok, _, _Leader} ->
            ok;
        {error, already_member} ->
            ok;
        Error ->
            {error, recoverable, Error}
    end.

%% @doc Remove a local server from the shard cluster and clean up on-disk data.
%% It's required to have the local server running before calling this function.
%% This function is idempotent.
-spec drop_local_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    ok | emqx_ds:error(_Reason).
drop_local_server(DB, Shard) ->
    %% NOTE: Timeouts are ignored, it's a best effort attempt.
    _ = prep_stop_server(DB, Shard),
    LocalServer = local_server(DB, Shard),
    case remove_server(DB, Shard, LocalServer) of
        ok ->
            ra:force_delete_server(DB, LocalServer);
        {error, _, _Reason} = Error ->
            Error
    end.

%% @doc Remove a (remote) server from the shard cluster.
%% The server might not be running when calling this function, e.g. the node
%% might be offline. Because of this, on-disk data will not be cleaned up.
%% This function is idempotent.
-spec remove_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), server()) ->
    ok | emqx_ds:error(_Reason).
remove_server(DB, Shard, Server) ->
    ShardServers = shard_servers(DB, Shard),
    Timeout = ?MEMBERSHIP_CHANGE_TIMEOUT,
    case ra_try_servers(ShardServers, fun ra:remove_member/3, [Server, Timeout]) of
        {ok, _, _Leader} ->
            ok;
        {error, not_member} ->
            ok;
        Error ->
            {error, recoverable, Error}
    end.

-spec server_info
    (readiness, server()) -> ready | {unready, _Status, _Membership} | unknown;
    (leader, server()) -> server() | unknown;
    (uid, server()) -> _UID :: binary() | unknown.
server_info(readiness, Server) ->
    %% NOTE
    %% Server is ready if it's either the leader or a follower with voter "membership"
    %% status (meaning it was promoted after catching up with the log).
    case current_leader(Server) of
        Server ->
            ready;
        Leader when Leader /= unknown ->
            member_info(readiness, Server, Leader);
        unknown ->
            unknown
    end;
server_info(leader, Server) ->
    current_leader(Server);
server_info(uid, Server) ->
    maps:get(uid, ra_overview(Server), unknown).

member_info(readiness, Server, Leader) ->
    Cluster = maps:get(cluster, ra_overview(Leader), #{}),
    member_readiness(maps:get(Server, Cluster, #{})).

current_leader(Server) ->
    %% NOTE: This call will block until the leader is known, or until the timeout.
    case ra:members(Server) of
        {ok, _Servers, Leader} ->
            Leader;
        _Error ->
            unknown
    end.

member_readiness(#{status := Status, voter_status := #{membership := Membership}}) ->
    case Status of
        normal when Membership =:= voter ->
            ready;
        _Other ->
            {unready, Status, Membership}
    end;
member_readiness(#{}) ->
    unknown.

%%

ra_try_servers([Server | Rest], Fun, Args) ->
    case erlang:apply(Fun, [Server | Args]) of
        {ok, R, Leader} ->
            {ok, R, Leader};
        {error, Reason} when Reason == noproc; Reason == nodedown ->
            ra_try_servers(Rest, Fun, Args);
        ErrorOrTimeout ->
            ErrorOrTimeout
    end;
ra_try_servers([], _Fun, _Args) ->
    {error, servers_unreachable}.

ra_overview(Server) ->
    case ra:member_overview(Server) of
        {ok, Overview, _Leader} ->
            Overview;
        _Error ->
            #{}
    end.

%%

init({DB, Shard, Opts}) ->
    _ = process_flag(trap_exit, true),
    ok = start_server(DB, Shard, Opts),
    {ok, {DB, Shard}}.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, {DB, Shard}) ->
    %% NOTE: Timeouts are ignored, it's a best effort attempt.
    catch prep_stop_server(DB, Shard),
    LocalServer = get_local_server(DB, Shard),
    ok = ra:stop_server(DB, LocalServer).

%%

start_server(DB, Shard, #{replication_options := ReplicationOpts}) ->
    ClusterName = cluster_name(DB, Shard),
    LocalServer = local_server(DB, Shard),
    Servers = shard_servers(DB, Shard),
    MutableConfig = #{tick_timeout => 100},
    case ra:restart_server(DB, LocalServer, MutableConfig) of
        {error, name_not_registered} ->
            Bootstrap = true,
            Machine = {module, emqx_ds_replication_layer, #{db => DB, shard => Shard}},
            LogOpts = maps:with(
                [
                    snapshot_interval,
                    resend_window
                ],
                ReplicationOpts
            ),
            ok = ra:start_server(DB, MutableConfig#{
                id => LocalServer,
                uid => server_uid(DB, Shard),
                cluster_name => ClusterName,
                initial_members => Servers,
                machine => Machine,
                log_init_args => LogOpts
            });
        ok ->
            Bootstrap = false;
        {error, {already_started, _}} ->
            Bootstrap = false
    end,
    %% NOTE
    %% Triggering election is necessary when a new consensus group is being brought up.
    %% TODO
    %% It's probably a good idea to rebalance leaders across the cluster from time to
    %% time. There's `ra:transfer_leadership/2` for that.
    try Bootstrap andalso ra:trigger_election(LocalServer, _Timeout = 1_000) of
        false ->
            ok;
        ok ->
            ok
    catch
        %% TODO
        %% Tolerating exceptions because server might be occupied with log replay for
        %% a while.
        exit:{timeout, _} when not Bootstrap ->
            ok
    end.

server_uid(_DB, Shard) ->
    %% NOTE
    %% Each new "instance" of a server should have a unique identifier. Otherwise,
    %% if some server migrates to another node during rebalancing, and then comes
    %% back, `ra` will be very confused by it having the same UID as before.
    %% Keeping the shard ID as a prefix to make it easier to identify the server
    %% in the filesystem / logs / etc.
    Ts = integer_to_binary(erlang:system_time(microsecond)),
    <<Shard/binary, "_", Ts/binary>>.

%%

prep_stop_server(DB, Shard) ->
    prep_stop_server(DB, Shard, 5_000).

prep_stop_server(DB, Shard, Timeout) ->
    LocalServer = get_local_server(DB, Shard),
    Candidates = lists:delete(LocalServer, shard_servers(DB, Shard)),
    case lookup_leader(DB, Shard) of
        LocalServer when Candidates =/= [] ->
            %% NOTE
            %% Trigger leadership transfer *and* force to wait until the new leader
            %% is elected and updated in the leaderboard. This should help to avoid
            %% edge cases where entries appended right before removal are duplicated
            %% due to client retries.
            %% TODO: Candidate may be offline.
            [Candidate | _] = Candidates,
            _ = ra:transfer_leadership(LocalServer, Candidate),
            wait_until(fun() -> lookup_leader(DB, Shard) == Candidate end, Timeout);
        _Another ->
            ok
    end.

%%

memoize(Fun, Args) ->
    %% NOTE: Assuming that the function is pure and never returns `undefined`.
    case persistent_term:get([Fun | Args], undefined) of
        undefined ->
            Result = erlang:apply(Fun, Args),
            _ = persistent_term:put([Fun | Args], Result),
            Result;
        Result ->
            Result
    end.

wait_until(Fun, Timeout) ->
    wait_until(Fun, Timeout, 100).

wait_until(Fun, Timeout, Sleep) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    loop_until(Fun, Deadline, Sleep).

loop_until(Fun, Deadline, Sleep) ->
    case Fun() of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) of
                Now when Now < Deadline ->
                    timer:sleep(Sleep),
                    loop_until(Fun, Deadline, Sleep);
                _ ->
                    timeout
            end
    end.
