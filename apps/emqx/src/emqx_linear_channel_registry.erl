%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc linearizable channel registry
%% A channel registry fo
%% @end
-module(emqx_linear_channel_registry).

-behaviour(gen_server).

-include("emqx.hrl").
-include("types.hrl").
-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export_type([lcr_channel/0]).

-export([
    start_link/0,
    is_enabled/0,
    lookup_channels_d/1,
    max_channel_d/1,
    register_channel/3
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

-record(lcr_channel, {
    id :: channel_id(),
    pid :: pid(),
    vsn :: integer(),
    %% reserved for future use
    prop :: undefined
}).

-type lcr_channel() :: #lcr_channel{}.

-type channel_id() :: emqx_types:clientid().

%%% API

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([broker, enable_linear_channel_registry], false).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_channel(#lcr_channel{id = ClientId, vsn = MyVsn} = Ch, CachedMax) ->
    Res = mria:transaction(
        ?LCR_SHARD,
        fun() ->
            %% Read from source of truth
            OtherChannels = mnesia:read(?LCR_TAB, ClientId, write),
            case max_channel(OtherChannels) of
                #lcr_channel{vsn = LatestVsn} when LatestVsn > MyVsn ->
                    mnesia:abort(channel_outdated);
                %% @NOTE: include `undefined'
                CachedMax ->
                    %% took over or discarded the correct version
                    mnesia:write(?LCR_TAB, Ch, write),
                    ok;
                #lcr_channel{} = NewerChannel ->
                    %% Takeover from wrong session, abort and restart
                    mnesia:abort({restart_takeover, NewerChannel, CachedMax, MyVsn})
            end
        end
    ),
    case Res of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

register_channel(
    #{clientid := ClientId, predecessor := CachedMax},
    Pid,
    #{trpt_started_at := TsMs}
) ->
    Ch = #lcr_channel{
        id = ClientId,
        pid = Pid,
        vsn = TsMs,
        prop = undefined
    },
    register_channel(Ch, CachedMax).

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
        #lcr_channel{pid = '$1', vsn = 0},
        Channels
    ).

ch_pid(undefined) ->
    undefined;
ch_pid(#lcr_channel{pid = Pid}) ->
    Pid.

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

cleanup_channels(_) ->
    %% @TODO maybe not needed to handle here
    ok.
