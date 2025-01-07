%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% TODO: remove the v1 table in release-590

-module(emqx_exclusive_subscription).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    check_subscribe/2,
    unsubscribe/2,
    dirty_lookup_clientid/1,
    clear/0
]).

%% Internal exports (RPC)
-export([
    try_subscribe/3,
    do_cleanup_subscriptions/1
]).

-record(exclusive_subscription, {
    topic :: emqx_types:topic(),
    clientid :: emqx_types:clientid()
}).

-record(exclusive_subscription_v2, {
    topic :: emqx_types:topic(),
    clientid :: emqx_types:clientid(),
    node :: node(),
    extra = #{} :: map()
}).

-define(TAB, emqx_exclusive_subscription).
-define(TAB_V2, emqx_exclusive_subscription_v2).
-define(EXCLUSIVE_SHARD, emqx_exclusive_shard).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

init_tables() ->
    StoreProps = [
        {ets, [
            {read_concurrency, true},
            {write_concurrency, true}
        ]}
    ],

    ok = mria:create_table(?TAB, [
        {rlog_shard, ?EXCLUSIVE_SHARD},
        {type, set},
        {storage, ram_copies},
        {record_name, exclusive_subscription},
        {attributes, record_info(fields, exclusive_subscription)},
        {storage_properties, StoreProps}
    ]),

    ok = mria:create_table(?TAB_V2, [
        {rlog_shard, ?EXCLUSIVE_SHARD},
        {type, set},
        {storage, ram_copies},
        {record_name, exclusive_subscription_v2},
        {attributes, record_info(fields, exclusive_subscription_v2)},
        {storage_properties, StoreProps}
    ]),

    ok = mria:wait_for_tables([?TAB, ?TAB_V2]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec check_subscribe(emqx_types:clientinfo(), emqx_types:topic()) ->
    allow | deny.
check_subscribe(#{clientid := ClientId}, Topic) ->
    case
        mria:transaction(?EXCLUSIVE_SHARD, fun ?MODULE:try_subscribe/3, [ClientId, Topic, node()])
    of
        {atomic, Res} ->
            Res;
        {aborted, Reason} ->
            ?SLOG(warning, #{
                msg => "check_subscribe_aborted", topic => Topic, reason => Reason
            }),
            deny
    end.

unsubscribe(Topic, #{is_exclusive := true}) ->
    _ = mria:transaction(?EXCLUSIVE_SHARD, fun mnesia:delete/1, [{?TAB, Topic}]),
    _ = mria:transaction(?EXCLUSIVE_SHARD, fun mnesia:delete/1, [{?TAB_V2, Topic}]),
    ok;
unsubscribe(_Topic, _SubOpts) ->
    ok.

dirty_lookup_clientid(Topic) ->
    case mnesia:dirty_read(?TAB, Topic) of
        [#exclusive_subscription{clientid = ClientId}] ->
            ClientId;
        _ ->
            undefined
    end.

clear() ->
    _ = mria:clear_table(?TAB),
    _ = mria:clear_table(?TAB_V2),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    init_tables(),
    ok = ekka:monitor(membership),
    {ok, #{}}.

handle_call(Request, From, State) ->
    ?SLOG(warning, #{
        msg => "unexpected_call",
        call => Request,
        from => From
    }),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?SLOG(warning, #{
        msg => "unexpected_cast",
        cast => Request
    }),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    cleanup_subscriptions(Node),
    {noreply, State};
handle_info({membership, {node, down, Node}}, State) ->
    cleanup_subscriptions(Node),
    {noreply, State};
handle_info({membership, _}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(warning, #{
        msg => "exclusive_sub_worker_unexpected_info",
        info => Info
    }),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = ekka:unmonitor(membership).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

try_subscribe(ClientId, Topic, Node) ->
    case mnesia:wread({?TAB, Topic}) of
        [] ->
            mnesia:write(
                ?TAB,
                #exclusive_subscription{
                    clientid = ClientId,
                    topic = Topic
                },
                write
            ),
            mnesia:write(
                ?TAB_V2,
                #exclusive_subscription_v2{
                    clientid = ClientId,
                    topic = Topic,
                    node = Node
                },
                write
            ),
            allow;
        [#exclusive_subscription{clientid = ClientId, topic = Topic}] ->
            %% Fixed the issue-13476
            %% In this feature, the user must manually call `unsubscribe` to release the lock,
            %% but sometimes the node may go down for some reason,
            %% then the client will reconnect to this node and resubscribe.
            %% We need to allow resubscription, otherwise the lock will never be released.
            allow;
        [_] ->
            deny
    end.

cleanup_subscriptions(Node) ->
    global:trans(
        {{?MODULE, ?FUNCTION_NAME}, self()},
        fun() ->
            mria:transaction(?EXCLUSIVE_SHARD, fun ?MODULE:do_cleanup_subscriptions/1, [Node])
        end
    ).

do_cleanup_subscriptions(Node0) ->
    Spec = ets:fun2ms(fun(#exclusive_subscription_v2{node = Node} = Data) when
        Node0 =:= Node
    ->
        Data
    end),
    lists:foreach(
        fun(#exclusive_subscription_v2{topic = Topic} = Obj) ->
            mnesia:delete({?TAB, Topic}),
            mnesia:delete_object(?TAB_V2, Obj, write)
        end,
        mnesia:select(?TAB_V2, Spec, write)
    ).
