%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Broker]").

-export([start_link/2]).

%% PubSub
-export([ subscribe/1
        , subscribe/2
        , subscribe/3
        ]).

-export([unsubscribe/1]).

-export([subscriber_down/1]).

-export([ publish/1
        , safe_publish/1
        ]).

-export([dispatch/2]).

%% PubSub Infos
-export([ subscriptions/1
        , subscribers/1
        , subscribed/2
        ]).

-export([ get_subopts/2
        , set_subopts/2
        ]).

-export([topics/0]).

%% Stats fun
-export([stats_fun/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-import(emqx_tables, [lookup_value/2, lookup_value/3]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(BROKER, ?MODULE).

%% ETS tables for PubSub
-define(SUBOPTION, emqx_suboption).
-define(SUBSCRIBER, emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).

%% Guards
-define(is_subid(Id), (is_binary(Id) orelse is_atom(Id))).

-spec(start_link(atom(), pos_integer()) -> startlink_ret()).
start_link(Pool, Id) ->
    ok = create_tabs(),
    gen_server:start_link({local, emqx_misc:proc_name(?BROKER, Id)},
                          ?MODULE, [Pool, Id], []).

%%------------------------------------------------------------------------------
%% Create tabs
%%------------------------------------------------------------------------------

-spec(create_tabs() -> ok).
create_tabs() ->
    TabOpts = [public, {read_concurrency, true}, {write_concurrency, true}],

    %% SubOption: {SubPid, Topic} -> SubOption
    ok = emqx_tables:new(?SUBOPTION, [set | TabOpts]),

    %% Subscription: SubPid -> Topic1, Topic2, Topic3, ...
    %% duplicate_bag: o(1) insert
    ok = emqx_tables:new(?SUBSCRIPTION, [duplicate_bag | TabOpts]),

    %% Subscriber: Topic -> SubPid1, SubPid2, SubPid3, ...
    %% bag: o(n) insert:(
    ok = emqx_tables:new(?SUBSCRIBER, [bag | TabOpts]).

%%------------------------------------------------------------------------------
%% Subscribe API
%%------------------------------------------------------------------------------

-spec(subscribe(emqx_topic:topic()) -> ok).
subscribe(Topic) when is_binary(Topic) ->
    subscribe(Topic, undefined).

-spec(subscribe(emqx_topic:topic(), emqx_types:subid() | emqx_types:subopts()) -> ok).
subscribe(Topic, SubId) when is_binary(Topic), ?is_subid(SubId) ->
    subscribe(Topic, SubId, #{qos => 0});
subscribe(Topic, SubOpts) when is_binary(Topic), is_map(SubOpts) ->
    subscribe(Topic, undefined, SubOpts).

-spec(subscribe(emqx_topic:topic(), emqx_types:subid(), emqx_types:subopts()) -> ok).
subscribe(Topic, SubId, SubOpts) when is_binary(Topic), ?is_subid(SubId), is_map(SubOpts) ->
    case ets:member(?SUBOPTION, {SubPid = self(), Topic}) of
        false -> %% New
            ok = emqx_broker_helper:register_sub(SubPid, SubId),
            do_subscribe(Topic, SubPid, with_subid(SubId, SubOpts));
        true -> %% Existed
            set_subopts(SubPid, Topic, with_subid(SubId, SubOpts)),
            ok %% ensure to return 'ok'
    end.

-compile({inline, [with_subid/2]}).
with_subid(undefined, SubOpts) ->
    SubOpts;
with_subid(SubId, SubOpts) ->
    maps:put(subid, SubId, SubOpts).

%% @private
do_subscribe(Topic, SubPid, SubOpts) ->
    true = ets:insert(?SUBSCRIPTION, {SubPid, Topic}),
    Group = maps:get(share, SubOpts, undefined),
    do_subscribe(Group, Topic, SubPid, SubOpts).

do_subscribe(undefined, Topic, SubPid, SubOpts) ->
    case emqx_broker_helper:get_sub_shard(SubPid, Topic) of
        0 -> true = ets:insert(?SUBSCRIBER, {Topic, SubPid}),
             true = ets:insert(?SUBOPTION, {{SubPid, Topic}, SubOpts}),
             call(pick(Topic), {subscribe, Topic});
        I -> true = ets:insert(?SUBSCRIBER, {{shard, Topic, I}, SubPid}),
             true = ets:insert(?SUBOPTION, {{SubPid, Topic}, maps:put(shard, I, SubOpts)}),
             call(pick({Topic, I}), {subscribe, Topic, I})
    end;

%% Shared subscription
do_subscribe(Group, Topic, SubPid, SubOpts) ->
    true = ets:insert(?SUBOPTION, {{SubPid, Topic}, SubOpts}),
    emqx_shared_sub:subscribe(Group, Topic, SubPid).

%%--------------------------------------------------------------------
%% Unsubscribe API
%%--------------------------------------------------------------------

-spec(unsubscribe(emqx_topic:topic()) -> ok).
unsubscribe(Topic) when is_binary(Topic) ->
    SubPid = self(),
    case ets:lookup(?SUBOPTION, {SubPid, Topic}) of
        [{_, SubOpts}] ->
            _ = emqx_broker_helper:reclaim_seq(Topic),
            do_unsubscribe(Topic, SubPid, SubOpts);
        [] -> ok
    end.

do_unsubscribe(Topic, SubPid, SubOpts) ->
    true = ets:delete(?SUBOPTION, {SubPid, Topic}),
    true = ets:delete_object(?SUBSCRIPTION, {SubPid, Topic}),
    Group = maps:get(share, SubOpts, undefined),
    do_unsubscribe(Group, Topic, SubPid, SubOpts).

do_unsubscribe(undefined, Topic, SubPid, SubOpts) ->
    case maps:get(shard, SubOpts, 0) of
        0 -> true = ets:delete_object(?SUBSCRIBER, {Topic, SubPid}),
             cast(pick(Topic), {unsubscribed, Topic});
        I -> true = ets:delete_object(?SUBSCRIBER, {{shard, Topic, I}, SubPid}),
             cast(pick({Topic, I}), {unsubscribed, Topic, I})
    end;

do_unsubscribe(Group, Topic, SubPid, _SubOpts) ->
    emqx_shared_sub:unsubscribe(Group, Topic, SubPid).

%%--------------------------------------------------------------------
%% Publish
%%--------------------------------------------------------------------

-spec(publish(emqx_types:message()) -> emqx_types:publish_result()).
publish(Msg) when is_record(Msg, message) ->
    _ = emqx_tracer:trace(publish, Msg),
    emqx_message:is_sys(Msg) orelse emqx_metrics:inc('messages.publish'),
    case emqx_hooks:run_fold('message.publish', [], emqx_message:clean_dup(Msg)) of
        #message{headers = #{allow_publish := false}} ->
            ?LOG(notice, "Stop publishing: ~s", [emqx_message:format(Msg)]),
            [];
        Msg1 = #message{topic = Topic} ->
            route(aggre(emqx_router:match_routes(Topic)), delivery(Msg1))
    end.

%% Called internally
-spec(safe_publish(emqx_types:message()) -> emqx_types:publish_result()).
safe_publish(Msg) when is_record(Msg, message) ->
    try
        publish(Msg)
    catch
        _:Error:Stk->
            ?LOG(error, "Publish error: ~0p~n~s~n~0p",
                 [Error, emqx_message:format(Msg), Stk])
    after
        []
    end.

-compile({inline, [delivery/1]}).
delivery(Msg) -> #delivery{sender = self(), message = Msg}.

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-spec(route([emqx_types:route_entry()], emqx_types:delivery())
      -> emqx_types:publish_result()).
route([], #delivery{message = Msg}) ->
    ok = emqx_hooks:run('message.dropped', [Msg, #{node => node()}, no_subscribers]),
    ok = inc_dropped_cnt(Msg),
    [];

route(Routes, Delivery) ->
    lists:foldl(fun(Route, Acc) ->
                        [do_route(Route, Delivery) | Acc]
                end, [], Routes).

do_route({To, Node}, Delivery) when Node =:= node() ->
    {Node, To, dispatch(To, Delivery)};
do_route({To, Node}, Delivery) when is_atom(Node) ->
    {Node, To, forward(Node, To, Delivery, emqx:get_env(rpc_mode, async))};
do_route({To, Group}, Delivery) when is_tuple(Group); is_binary(Group) ->
    {share, To, emqx_shared_sub:dispatch(Group, To, Delivery)}.

aggre([]) -> [];
aggre([#route{topic = To, dest = Node}]) when is_atom(Node) ->
    [{To, Node}];
aggre([#route{topic = To, dest = {Group, _Node}}]) ->
    [{To, Group}];
aggre(Routes) ->
    lists:foldl(
      fun(#route{topic = To, dest = Node}, Acc) when is_atom(Node) ->
          [{To, Node} | Acc];
         (#route{topic = To, dest = {Group, _Node}}, Acc) ->
          lists:usort([{To, Group} | Acc])
      end, [], Routes).

%% @doc Forward message to another node.
-spec(forward(node(), emqx_types:topic(), emqx_types:delivery(), RpcMode::sync|async)
    -> emqx_types:deliver_result()).
forward(Node, To, Delivery, async) ->
    case emqx_rpc:cast(To, Node, ?BROKER, dispatch, [To, Delivery]) of
        true -> emqx_metrics:inc('messages.forward');
        {badrpc, Reason} ->
            ?LOG(error, "Ansync forward msg to ~s failed due to ~p", [Node, Reason]),
            {error, badrpc}
    end;

forward(Node, To, Delivery, sync) ->
    case emqx_rpc:call(To, Node, ?BROKER, dispatch, [To, Delivery]) of
        {badrpc, Reason} ->
            ?LOG(error, "Sync forward msg to ~s failed due to ~p", [Node, Reason]),
            {error, badrpc};
        Result ->
            emqx_metrics:inc('messages.forward'), Result
    end.

-spec(dispatch(emqx_topic:topic(), emqx_types:delivery()) -> emqx_types:deliver_result()).
dispatch(Topic, #delivery{message = Msg}) ->
    case subscribers(Topic) of
        [] -> ok = emqx_hooks:run('message.dropped', [Msg, #{node => node()}, no_subscribers]),
              ok = inc_dropped_cnt(Topic),
              {error, no_subscribers};
        [Sub] -> %% optimize?
            dispatch(Sub, Topic, Msg);
        Subs  -> lists:foldl(
                   fun(Sub, Res) ->
                           case dispatch(Sub, Topic, Msg) of
                               ok -> Res;
                               Err -> Err
                           end
                   end, ok, Subs)
    end.

dispatch(SubPid, Topic, Msg) when is_pid(SubPid) ->
    case erlang:is_process_alive(SubPid) of
        true ->
            SubPid ! {deliver, Topic, Msg},
            ok;
        false -> {error, subscriber_die}
    end;

dispatch({shard, I}, Topic, Msg) ->
    lists:foldl(
        fun(SubPid, Res) ->
            case dispatch(SubPid, Topic, Msg) of
                ok -> Res;
                Err -> Err
            end
        end, ok, subscribers({shard, Topic, I})).

-compile({inline, [inc_dropped_cnt/1]}).
inc_dropped_cnt(Msg) ->
    case emqx_message:is_sys(Msg) of
        true  -> ok;
        false -> ok = emqx_metrics:inc('messages.dropped'),
                 emqx_metrics:inc('messages.dropped.no_subscribers')
    end.

-compile({inline, [subscribers/1]}).
-spec(subscribers(emqx_topic:topic()) -> [pid()]).
subscribers(Topic) when is_binary(Topic) ->
    lookup_value(?SUBSCRIBER, Topic, []);
subscribers(Shard = {shard, _Topic, _I})  ->
    lookup_value(?SUBSCRIBER, Shard, []).

%%--------------------------------------------------------------------
%% Subscriber is down
%%--------------------------------------------------------------------

-spec(subscriber_down(pid()) -> true).
subscriber_down(SubPid) ->
    lists:foreach(
      fun(Topic) ->
          case lookup_value(?SUBOPTION, {SubPid, Topic}) of
              SubOpts when is_map(SubOpts) ->
                  _ = emqx_broker_helper:reclaim_seq(Topic),
                  true = ets:delete(?SUBOPTION, {SubPid, Topic}),
                  case maps:get(shard, SubOpts, 0) of
                      0 -> true = ets:delete_object(?SUBSCRIBER, {Topic, SubPid}),
                           ok = cast(pick(Topic), {unsubscribed, Topic});
                      I -> true = ets:delete_object(?SUBSCRIBER, {{shard, Topic, I}, SubPid}),
                           ok = cast(pick({Topic, I}), {unsubscribed, Topic, I})
                  end;
              undefined -> ok
          end
      end, lookup_value(?SUBSCRIPTION, SubPid, [])),
    ets:delete(?SUBSCRIPTION, SubPid).

%%--------------------------------------------------------------------
%% Management APIs
%%--------------------------------------------------------------------

-spec(subscriptions(pid() | emqx_types:subid())
      -> [{emqx_topic:topic(), emqx_types:subopts()}]).
subscriptions(SubPid) when is_pid(SubPid) ->
    [{Topic, lookup_value(?SUBOPTION, {SubPid, Topic}, #{})}
      || Topic <- lookup_value(?SUBSCRIPTION, SubPid, [])];
subscriptions(SubId) ->
    case emqx_broker_helper:lookup_subpid(SubId) of
        SubPid when is_pid(SubPid) ->
            subscriptions(SubPid);
        undefined -> []
    end.

-spec(subscribed(pid(), emqx_topic:topic()) -> boolean()).
subscribed(SubPid, Topic) when is_pid(SubPid) ->
    ets:member(?SUBOPTION, {SubPid, Topic});
subscribed(SubId, Topic) when ?is_subid(SubId) ->
    SubPid = emqx_broker_helper:lookup_subpid(SubId),
    ets:member(?SUBOPTION, {SubPid, Topic}).

-spec(get_subopts(pid(), emqx_topic:topic()) -> maybe(emqx_types:subopts())).
get_subopts(SubPid, Topic) when is_pid(SubPid), is_binary(Topic) ->
    lookup_value(?SUBOPTION, {SubPid, Topic});
get_subopts(SubId, Topic) when ?is_subid(SubId) ->
    case emqx_broker_helper:lookup_subpid(SubId) of
        SubPid when is_pid(SubPid) ->
            get_subopts(SubPid, Topic);
        undefined -> undefined
    end.

-spec(set_subopts(emqx_topic:topic(), emqx_types:subopts()) -> boolean()).
set_subopts(Topic, NewOpts) when is_binary(Topic), is_map(NewOpts) ->
    set_subopts(self(), Topic, NewOpts).

%% @private
set_subopts(SubPid, Topic, NewOpts) ->
    Sub = {SubPid, Topic},
    case ets:lookup(?SUBOPTION, Sub) of
        [{_, OldOpts}] ->
            ets:insert(?SUBOPTION, {Sub, maps:merge(OldOpts, NewOpts)});
        [] -> false
    end.

-spec(topics() -> [emqx_topic:topic()]).
topics() ->
    emqx_router:topics().

%%--------------------------------------------------------------------
%% Stats fun
%%--------------------------------------------------------------------

stats_fun() ->
    safe_update_stats(?SUBSCRIBER, 'subscribers.count', 'subscribers.max'),
    safe_update_stats(?SUBSCRIPTION, 'subscriptions.count', 'subscriptions.max'),
    safe_update_stats(?SUBOPTION, 'suboptions.count', 'suboptions.max').

safe_update_stats(Tab, Stat, MaxStat) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

%%--------------------------------------------------------------------
%% call, cast, pick
%%--------------------------------------------------------------------

-compile({inline, [call/2, cast/2, pick/1]}).

call(Broker, Req) ->
    gen_server:call(Broker, Req).

cast(Broker, Msg) ->
    gen_server:cast(Broker, Msg).

%% Pick a broker
pick(Topic) ->
    gproc_pool:pick_worker(broker_pool, Topic).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({subscribe, Topic}, _From, State) ->
    Ok = emqx_router:do_add_route(Topic),
    {reply, Ok, State};

handle_call({subscribe, Topic, I}, _From, State) ->
    Ok = case get(Shard = {Topic, I}) of
             undefined ->
                 _ = put(Shard, true),
                 true = ets:insert(?SUBSCRIBER, {Topic, {shard, I}}),
                 cast(pick(Topic), {subscribe, Topic});
             true -> ok
         end,
    {reply, Ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({subscribe, Topic}, State) ->
    case emqx_router:do_add_route(Topic) of
        ok -> ok;
        {error, Reason} ->
            ?LOG(error, "Failed to add route: ~p", [Reason])
    end,
    {noreply, State};

handle_cast({unsubscribed, Topic}, State) ->
    case ets:member(?SUBSCRIBER, Topic) of
        false ->
            _ = emqx_router:do_delete_route(Topic);
        true -> ok
    end,
    {noreply, State};

handle_cast({unsubscribed, Topic, I}, State) ->
    case ets:member(?SUBSCRIBER, {shard, Topic, I}) of
        false ->
            _ = erase({Topic, I}),
            true = ets:delete_object(?SUBSCRIBER, {Topic, {shard, I}}),
            cast(pick(Topic), {unsubscribed, Topic});
        true -> ok
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

