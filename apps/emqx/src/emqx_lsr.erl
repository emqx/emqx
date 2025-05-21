%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_lsr).

-behaviour(gen_server).

-include("emqx.hrl").
-include_lib("emqx/include/emqx_lsr.hrl").
-include("types.hrl").
-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export_type([lsr_channel/0]).

-export([
    start_link/0,
    is_enabled/0,
    lookup_channels_d/1,
    max_channel_d/1,
    register_channel/3,
    unregister_channel/2,
    count_local_d/0,
    do_cleanup_channels/1
]).

%% getters
-export([ch_pid/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(LSR_TAB, ?MODULE).

-type lsr_channel() :: #lsr_channel{}.

%%% API

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([broker, enable_linear_channel_registry], false).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_channel(emqx_types:clientinfo(), pid(), emqx_types:conninfo()) ->
    ok | {error, any()}.
register_channel(
    #{clientid := ClientId, predecessor := CachedMax},
    Pid,
    #{trpt_started_at := TsMs}
) ->
    Ch = #lsr_channel{
        id = ClientId,
        pid = Pid,
        vsn = TsMs
    },
    do_register_channel(Ch, CachedMax).

-spec unregister_channel({emqx_types:clientid(), pid()}, non_neg_integer()) ->
    ok.
unregister_channel({ClientId, ChanPid}, Vsn) ->
    Ch = #lsr_channel{
        id = ClientId,
        pid = ChanPid,
        vsn = Vsn
    },
    mria:dirty_delete_object(?LSR_TAB, Ch),
    ok.

-spec lookup_channels_d(emqx_types:clientid()) -> [lsr_channel()].
lookup_channels_d(ClientId) ->
    mnesia:dirty_read(?LSR_TAB, ClientId).

%% @doc dirty read local max channel
-spec max_channel_d(emqx_types:clientid()) -> option(lsr_channel()).
max_channel_d(ClientId) ->
    max_channel(lookup_channels_d(ClientId)).

%% @doc find last channel with the highest version
-spec max_channel([lsr_channel()]) -> option(lsr_channel()).
max_channel([]) ->
    undefined;
max_channel(Channels) ->
    lists:foldl(
        fun
            (#lsr_channel{vsn = Vsn, pid = Pid} = This, #lsr_channel{vsn = Max}) when
                Vsn > Max andalso is_pid(Pid)
            ->
                This;
            (_, Max) ->
                Max
        end,
        hd(Channels),
        Channels
    ).

-spec ch_pid(option(lsr_channel())) -> option(pid()).
ch_pid(undefined) ->
    undefined;
ch_pid(#lsr_channel{pid = Pid}) ->
    Pid.

-spec count_local_d() -> non_neg_integer().
count_local_d() ->
    mnesia:table_info(?LSR_TAB, size).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    SHARD = ?LSR_SHARD,
    mria_config:set_dirty_shard(SHARD, true),
    ok = mria:create_table(?LSR_TAB, [
        {type, bag},
        {rlog_shard, ?LSR_SHARD},
        {storage, ram_copies},
        {record_name, lsr_channel},
        {attributes, record_info(fields, lsr_channel)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria_rlog:wait_for_shards([SHARD], infinity),
    ok = ekka:monitor(membership),

    maybe_init_channel_cleanup(),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    ?tp(warning, lsr_mnesia_down, #{self_node => node(), down_node => Node}),
    maybe_cleanup_channels(Node),
    {noreply, State};
handle_info({membership, {node, down, Node}}, State) ->
    ?tp(warning, lsr_node_down, #{self_node => node(), down_node => Node}),
    maybe_cleanup_channels(Node),
    {noreply, State};
handle_info({membership, _Event}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec maybe_cleanup_channels(node()) -> ok.
maybe_cleanup_channels(Node) ->
    case if_cleanup_channels(Node) of
        true ->
            ?tp(warning, lsr_node_down_cleanup, #{self_node => node(), down_node => Node}),
            do_cleanup_channels(Node);
        false ->
            ?tp(debug, lsr_node_down_no_cleanup, #{self_node => node(), down_node => Node}),
            ok
    end.

do_cleanup_channels(Node) ->
    TS = erlang:system_time(),
    MatchSpec = [
        {
            #lsr_channel{pid = '$1', vsn = '$2', _ = '_'},
            _Match = [{'andalso', {'<', '$2', TS}, {'==', {node, '$1'}, Node}}],
            _Return = ['$_']
        }
    ],

    %% @TODO maybe use mnesia:match_delete/2
    mria:async_dirty(?LSR_SHARD, fun() ->
        do_cleanup_channels_cont(do_cleanup_channels_init(MatchSpec))
    end),
    ok.

do_cleanup_channels_init(MS) ->
    case mnesia:select(?LSR_TAB, MS, 200, write) of
        {Matched, Cont} ->
            lists:foreach(
                fun(Obj) ->
                    mnesia:delete_object(?LSR_TAB, Obj, write)
                end,
                Matched
            ),
            Cont;
        '$end_of_table' ->
            '$end_of_table'
    end.

do_cleanup_channels_cont('$end_of_table') ->
    ok;
do_cleanup_channels_cont(Cont0) ->
    do_cleanup_channels_cont(
        case mnesia:select(Cont0) of
            {Matched, Cont} ->
                lists:foreach(
                    fun(Obj) ->
                        mnesia:delete_object(?LSR_TAB, Obj, write)
                    end,
                    Matched
                ),
                Cont;
            '$end_of_table' ->
                '$end_of_table'
        end
    ).

-spec if_cleanup_channels(node()) -> boolean().
if_cleanup_channels(Node) ->
    case core =:= mria_rlog:role() of
        false ->
            false;
        true ->
            %% @NOTE mria:cluster_nodes(cores) only returns 'DOWN' cores calling from `core' node.
            case lists:member(Node, mria:cluster_nodes(cores)) of
                true ->
                    %%% Nobody will run cleanup for other core node because:
                    %%% 1. usually no traffic via core nodes while table scan is expensive.
                    %%% 2. core is designed and expected to comeback and clean up on its own.
                    false;
                false ->
                    Cores = lists:usort(mria_membership:running_core_nodelist()),
                    Hash = erlang:phash2(Node, length(Cores)),
                    lists:nth(Hash + 1, Cores) =:= node()
            end
    end.

%% Internals
do_register_channel(#lsr_channel{id = ClientId, vsn = MyVsn} = Ch, CachedMax) ->
    Res = mria:transaction(
        ?LSR_SHARD,
        fun() ->
            %% Read from source of truth
            OtherChannels = mnesia:read(?LSR_TAB, ClientId, write),
            case max_channel(OtherChannels) of
                undefined ->
                    mnesia:write(?LSR_TAB, Ch, write),
                    ok;
                CachedMax ->
                    %% took over or discarded the correct version
                    mnesia:write(?LSR_TAB, Ch, write),
                    ok;
                #lsr_channel{vsn = LatestVsn} when LatestVsn > MyVsn ->
                    mnesia:abort(?lsr_err_channel_outdated);
                #lsr_channel{} = NewerChannel ->
                    %% Takeover from wrong session, abort and restart
                    mnesia:abort({?lsr_err_restart_takeover, NewerChannel, CachedMax, MyVsn})
            end
        end
    ),
    case Res of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

maybe_init_channel_cleanup() ->
    case mria_rlog:role() of
        core ->
            do_cleanup_channels(node());
        _ ->
            ok
    end.
