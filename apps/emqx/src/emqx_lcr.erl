%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_lcr).

-behaviour(gen_server).

-include("emqx.hrl").
-include_lib("emqx/include/emqx_lcr.hrl").
-include("types.hrl").
-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export_type([lcr_channel/0]).

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

-define(LCR_TAB, ?MODULE).

-type lcr_channel() :: #lcr_channel{}.

%%% API

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([broker, enable_linear_channel_registry], false).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_channel(
    #{clientid := ClientId, predecessor := CachedMax},
    Pid,
    #{trpt_started_at := TsMs}
) ->
    Ch = #lcr_channel{
        id = ClientId,
        pid = Pid,
        vsn = TsMs
    },
    do_register_channel(Ch, CachedMax).

unregister_channel({ClientId, ChanPid}, Vsn) ->
    Ch = #lcr_channel{
        id = ClientId,
        pid = ChanPid,
        vsn = Vsn
    },
    mria:dirty_delete_object(?LCR_TAB, Ch),
    ok.

lookup_channels_d(ClientId) ->
    mnesia:dirty_read(?LCR_TAB, ClientId).

%% @doc dirty read local max channel
max_channel_d(ClientId) ->
    max_channel(lookup_channels_d(ClientId)).

%% @doc find last channel with the highest version
-spec max_channel([lcr_channel()]) -> option(lcr_channel()).
max_channel([]) ->
    undefined;
max_channel(Channels) ->
    lists:foldl(
        fun
            (#lcr_channel{vsn = Vsn, pid = Pid} = This, #lcr_channel{vsn = Max}) when
                Vsn > Max andalso is_pid(Pid)
            ->
                This;
            (_, Max) ->
                Max
        end,
        #lcr_channel{vsn = 0},
        Channels
    ).

ch_pid(undefined) ->
    undefined;
ch_pid(#lcr_channel{pid = Pid}) ->
    Pid.

count_local_d() ->
    try
        ets:info(?LCR_TAB, size)
    catch
        error:badarg ->
            0
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    SHARD = ?LCR_SHARD,
    mria_config:set_dirty_shard(SHARD, true),
    ok = mria:create_table(?LCR_TAB, [
        {type, bag},
        {rlog_shard, ?LCR_SHARD},
        {storage, ram_copies},
        {record_name, lcr_channel},
        {attributes, record_info(fields, lcr_channel)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria_rlog:wait_for_shards([SHARD], infinity),
    ok = ekka:monitor(membership),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    ?tp(warning, lcr_mnesia_down, #{node => Node}),
    cleanup_channels(Node),
    {noreply, State};
handle_info({membership, {node, down, Node}}, State) ->
    ?tp(warning, lcr_node_down, #{node => Node}),
    cleanup_channels(Node),
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

cleanup_channels(Node) ->
    case if_cleanup_channels(Node) of
        true ->
            do_cleanup_channels(Node);
        false ->
            emqx_logger:info("~p: NO Cleanup channels for node ~p", [?MODULE, Node]),
            ok
    end.

do_cleanup_channels(Node) ->
    emqx_logger:info("~p: Cleanup channels for node ~p", [?MODULE, Node]),
    TS = erlang:system_time(),
    MatchSpec = [
        {
            #lcr_channel{pid = '$1', vsn = '$2', _ = '_'},
            _Match = [{'andalso', {'<', '$2', TS}, {'==', {node, '$1'}, Node}}],
            _Return = ['$_']
        }
    ],

    mria:async_dirty(?LCR_SHARD, fun() ->
        do_cleanup_channels_cont(do_cleanup_channels_init(MatchSpec))
    end).

do_cleanup_channels_init(MS) ->
    case mnesia:select(?LCR_TAB, MS, 200, write) of
        {Matched, Cont} ->
            lists:foreach(
                fun(Obj) ->
                    mnesia:delete_object(?LCR_TAB, Obj, write)
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
                        mnesia:delete_object(?LCR_TAB, Obj, write)
                    end,
                    Matched
                ),
                Cont;
            '$end_of_table' ->
                '$end_of_table'
        end
    ).

if_cleanup_channels(Node) ->
    case core =:= mria_rlog:role() of
        false ->
            false;
        true ->
            Cores = lists:usort(mria_membership:running_core_nodelist()),
            Hash = erlang:phash2(Node, length(Cores)),
            lists:nth(Hash + 1, Cores) =:= node()
    end.

%% Internals
do_register_channel(#lcr_channel{id = ClientId, vsn = MyVsn} = Ch, CachedMax) ->
    Res = mria:transaction(
        ?LCR_SHARD,
        fun() ->
            %% Read from source of truth
            OtherChannels = mnesia:read(?LCR_TAB, ClientId, write),
            case max_channel(OtherChannels) of
                undefined ->
                    mnesia:write(?LCR_TAB, Ch, write),
                    ok;
                CachedMax ->
                    %% took over or discarded the correct version
                    mnesia:write(?LCR_TAB, Ch, write),
                    ok;
                #lcr_channel{vsn = LatestVsn} when LatestVsn > MyVsn ->
                    mnesia:abort(?lcr_err_channel_outdated);
                #lcr_channel{} = NewerChannel ->
                    %% Takeover from wrong session, abort and restart
                    mnesia:abort({?lcr_err_restart_takeover, NewerChannel, CachedMax, MyVsn})
            end
        end
    ),
    case Res of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.
