%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/2]).
-export([subscribe/1, subscribe/2, subscribe/3, subscribe/4]).
-export([multi_subscribe/1, multi_subscribe/2, multi_subscribe/3]).
-export([publish/1, safe_publish/1]).
-export([unsubscribe/1, unsubscribe/2, unsubscribe/3]).
-export([multi_unsubscribe/1, multi_unsubscribe/2, multi_unsubscribe/3]).
-export([dispatch/2, dispatch/3]).
-export([subscriptions/1, subscribers/1, subscribed/2]).
-export([get_subopts/2, set_subopts/3]).
-export([topics/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {pool, id, submap, submon}).
-record(subscribe, {topic, subpid, subid, subopts = #{}}).
-record(unsubscribe, {topic, subpid, subid}).

%% The default request timeout
-define(TIMEOUT, 60000).
-define(BROKER, ?MODULE).

%% ETS tables
-define(SUBOPTION,    emqx_suboption).
-define(SUBSCRIBER,   emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).

-define(is_subid(Id), (is_binary(Id) orelse is_atom(Id))).

-spec(start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)}, ?MODULE,
                          [Pool, Id], [{hibernate_after, 2000}]).

%%------------------------------------------------------------------------------
%% Subscribe
%%------------------------------------------------------------------------------

-spec(subscribe(topic()) -> ok).
subscribe(Topic) when is_binary(Topic) ->
    subscribe(Topic, self()).

-spec(subscribe(topic(), pid() | subid()) -> ok).
subscribe(Topic, SubPid) when is_binary(Topic), is_pid(SubPid) ->
    subscribe(Topic, SubPid, undefined);
subscribe(Topic, SubId) when is_binary(Topic), ?is_subid(SubId) ->
    subscribe(Topic, self(), SubId).

-spec(subscribe(topic(), pid() | subid(), subid() | subopts()) -> ok).
subscribe(Topic, SubPid, SubId) when is_binary(Topic), is_pid(SubPid), ?is_subid(SubId) ->
    subscribe(Topic, SubPid, SubId, #{});
subscribe(Topic, SubPid, SubId) when is_binary(Topic), is_pid(SubPid), ?is_subid(SubId) ->
    subscribe(Topic, SubPid, SubId, #{});
subscribe(Topic, SubPid, SubOpts) when is_binary(Topic), is_pid(SubPid), is_map(SubOpts) ->
    subscribe(Topic, SubPid, undefined, SubOpts);
subscribe(Topic, SubId, SubOpts) when is_binary(Topic), ?is_subid(SubId), is_map(SubOpts) ->
    subscribe(Topic, self(), SubId, SubOpts).

-spec(subscribe(topic(), pid(), subid(), subopts()) -> ok).
subscribe(Topic, SubPid, SubId, SubOpts) when is_binary(Topic), is_pid(SubPid),
                                              ?is_subid(SubId), is_map(SubOpts) ->
    Broker = pick(SubPid),
    SubReq = #subscribe{topic = Topic, subpid = SubPid, subid = SubId, subopts = SubOpts},
    wait_for_reply(async_call(Broker, SubReq), ?TIMEOUT).

-spec(multi_subscribe(topic_table()) -> ok).
multi_subscribe(TopicTable) when is_list(TopicTable) ->
    multi_subscribe(TopicTable, self()).

-spec(multi_subscribe(topic_table(), pid() | subid()) -> ok).
multi_subscribe(TopicTable, SubPid) when is_pid(SubPid) ->
    multi_subscribe(TopicTable, SubPid, undefined);
multi_subscribe(TopicTable, SubId) when ?is_subid(SubId) ->
    multi_subscribe(TopicTable, self(), SubId).

-spec(multi_subscribe(topic_table(), pid(), subid()) -> ok).
multi_subscribe(TopicTable, SubPid, SubId) when is_pid(SubPid), ?is_subid(SubId) ->
    Broker = pick(SubPid),
    SubReq = fun(Topic, SubOpts) ->
                 #subscribe{topic = Topic, subpid = SubPid, subid = SubId, subopts = SubOpts}
             end,
    wait_for_replies([async_call(Broker, SubReq(Topic, SubOpts))
                      || {Topic, SubOpts} <- TopicTable], ?TIMEOUT).

%%------------------------------------------------------------------------------
%% Unsubscribe
%%------------------------------------------------------------------------------

-spec(unsubscribe(topic()) -> ok).
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe(Topic, self()).

-spec(unsubscribe(topic(), pid() | subid()) -> ok).
unsubscribe(Topic, SubPid) when is_binary(Topic), is_pid(SubPid) ->
    unsubscribe(Topic, SubPid, undefined);
unsubscribe(Topic, SubId) when is_binary(Topic), ?is_subid(SubId) ->
    unsubscribe(Topic, self(), SubId).

-spec(unsubscribe(topic(), pid(), subid()) -> ok).
unsubscribe(Topic, SubPid, SubId) when is_binary(Topic), is_pid(SubPid), ?is_subid(SubId) ->
    Broker = pick(SubPid),
    UnsubReq = #unsubscribe{topic = Topic, subpid = SubPid, subid = SubId},
    wait_for_reply(async_call(Broker, UnsubReq), ?TIMEOUT).

-spec(multi_unsubscribe([topic()]) -> ok).
multi_unsubscribe(Topics) ->
    multi_unsubscribe(Topics, self()).

-spec(multi_unsubscribe([topic()], pid() | subid()) -> ok).
multi_unsubscribe(Topics, SubPid) when is_pid(SubPid) ->
    multi_unsubscribe(Topics, SubPid, undefined);
multi_unsubscribe(Topics, SubId) when ?is_subid(SubId) ->
    multi_unsubscribe(Topics, self(), SubId).

-spec(multi_unsubscribe([topic()], pid(), subid()) -> ok).
multi_unsubscribe(Topics, SubPid, SubId) when is_pid(SubPid), ?is_subid(SubId) ->
    Broker = pick(SubPid),
    UnsubReq = fun(Topic) ->
                   #unsubscribe{topic = Topic, subpid = SubPid, subid = SubId}
               end,
    wait_for_replies([async_call(Broker, UnsubReq(Topic)) || Topic <- Topics], ?TIMEOUT).

%%------------------------------------------------------------------------------
%% Publish
%%------------------------------------------------------------------------------

-spec(publish(message()) -> delivery()).
publish(Msg) when is_record(Msg, message) ->
    _ = emqx_tracer:trace(publish, Msg),
    case emqx_hooks:run('message.publish', [], Msg) of
        {ok, Msg1 = #message{topic = Topic}} ->
            route(aggre(emqx_router:match_routes(Topic)), delivery(Msg1));
        {stop, Msg1} ->
            emqx_logger:warning("Stop publishing: ~p", [Msg]), delivery(Msg1)
    end.

%% Called internally
safe_publish(Msg) when is_record(Msg, message) ->
    try
        publish(Msg)
    catch
        _:Error:Stacktrace ->
            emqx_logger:error("[Broker] publish error: ~p~n~p~n~p", [Error, Msg, Stacktrace])
    after
        ok
    end.

delivery(Msg) ->
    #delivery{sender = self(), message = Msg, flows = []}.

%%------------------------------------------------------------------------------
%% Route
%%------------------------------------------------------------------------------

route([], Delivery = #delivery{message = Msg}) ->
    emqx_hooks:run('message.dropped', [undefined, Msg]),
    dropped(Msg#message.topic), Delivery;

route([{To, Node}], Delivery) when Node =:= node() ->
    dispatch(To, Delivery);

route([{To, Node}], Delivery = #delivery{flows = Flows}) when is_atom(Node) ->
    forward(Node, To, Delivery#delivery{flows = [{route, Node, To}|Flows]});

route([{To, Shared}], Delivery) when is_tuple(Shared); is_binary(Shared) ->
    emqx_shared_sub:dispatch(Shared, To, Delivery);

route(Routes, Delivery) ->
    lists:foldl(fun(Route, Acc) -> route([Route], Acc) end, Delivery, Routes).

aggre([]) ->
    [];
aggre([#route{topic = To, dest = Dest}]) ->
    [{To, Dest}];
aggre(Routes) ->
    lists:foldl(
      fun(#route{topic = To, dest = Node}, Acc) when is_atom(Node) ->
          [{To, Node} | Acc];
        (#route{topic = To, dest = {Group, _Node}}, Acc) ->
          lists:usort([{To, Group} | Acc])
      end, [], Routes).

%% @doc Forward message to another node.
forward(Node, To, Delivery) ->
    %% rpc:call to ensure the delivery, but the latency:(
    case emqx_rpc:call(Node, ?BROKER, dispatch, [To, Delivery]) of
        {badrpc, Reason} ->
            emqx_logger:error("[Broker] Failed to forward msg to ~s: ~p", [Node, Reason]),
            Delivery;
        Delivery1 -> Delivery1
    end.

-spec(dispatch(topic(), delivery()) -> delivery()).
dispatch(Topic, Delivery = #delivery{message = Msg, flows = Flows}) ->
    case subscribers(Topic) of
        [] ->
            emqx_hooks:run('message.dropped', [undefined, Msg]),
            dropped(Topic), Delivery;
        [Sub] -> %% optimize?
            dispatch(Sub, Topic, Msg),
            Delivery#delivery{flows = [{dispatch, Topic, 1}|Flows]};
        Subscribers ->
            Count = lists:foldl(fun(Sub, Acc) ->
                                    dispatch(Sub, Topic, Msg), Acc + 1
                                end, 0, Subscribers),
            Delivery#delivery{flows = [{dispatch, Topic, Count}|Flows]}
    end.

dispatch({SubPid, _SubId}, Topic, Msg) when is_pid(SubPid) ->
    SubPid ! {dispatch, Topic, Msg};
dispatch({share, _Group, _Sub}, _Topic, _Msg) ->
    ignored.

dropped(<<"$SYS/", _/binary>>) ->
    ok;
dropped(_Topic) ->
    emqx_metrics:inc('messages/dropped').

-spec(subscribers(topic()) -> [subscriber()]).
subscribers(Topic) ->
    try ets:lookup_element(?SUBSCRIBER, Topic, 2) catch error:badarg -> [] end.

-spec(subscriptions(subscriber()) -> [{topic(), subopts()}]).
subscriptions(Subscriber) ->
    lists:map(fun({_, {share, _Group, Topic}}) ->
                  subscription(Topic, Subscriber);
                 ({_, Topic}) ->
                  subscription(Topic, Subscriber)
        end, ets:lookup(?SUBSCRIPTION, Subscriber)).

subscription(Topic, Subscriber) ->
    {Topic, ets:lookup_element(?SUBOPTION, {Topic, Subscriber}, 2)}.

-spec(subscribed(topic(), pid() | subid() | subscriber()) -> boolean()).
subscribed(Topic, SubPid) when is_binary(Topic), is_pid(SubPid) ->
    length(ets:match_object(?SUBOPTION, {{Topic, {SubPid, '_'}}, '_'}, 1)) == 1;
subscribed(Topic, SubId) when is_binary(Topic), ?is_subid(SubId) ->
    length(ets:match_object(?SUBOPTION, {{Topic, {'_', SubId}}, '_'}, 1)) == 1;
subscribed(Topic, {SubPid, SubId}) when is_binary(Topic), is_pid(SubPid), ?is_subid(SubId) ->
    ets:member(?SUBOPTION, {Topic, {SubPid, SubId}}).

-spec(get_subopts(topic(), subscriber()) -> subopts()).
get_subopts(Topic, Subscriber) when is_binary(Topic) ->
    try ets:lookup_element(?SUBOPTION, {Topic, Subscriber}, 2)
    catch error:badarg -> []
    end.

-spec(set_subopts(topic(), subscriber(), subopts()) -> boolean()).
set_subopts(Topic, Subscriber, Opts) when is_binary(Topic), is_map(Opts) ->
    case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
        [{_, OldOpts}] ->
            ets:insert(?SUBOPTION, {{Topic, Subscriber}, maps:merge(OldOpts, Opts)});
        [] -> false
    end.

async_call(Broker, Req) ->
    From = {self(), Tag = make_ref()},
    ok = gen_server:cast(Broker, {From, Req}),
    Tag.

wait_for_replies(Tags, Timeout) ->
    lists:foreach(
      fun(Tag) ->
          wait_for_reply(Tag, Timeout)
      end, Tags).

wait_for_reply(Tag, Timeout) ->
    receive
        {Tag, Reply} -> Reply
    after Timeout ->
        exit(timeout)
    end.

%% Pick a broker
pick(SubPid) when is_pid(SubPid) ->
    gproc_pool:pick_worker(broker, SubPid).

-spec(topics() -> [topic()]).
topics() -> emqx_router:topics().

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id, submap = #{}, submon = emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[Broker] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({From, #subscribe{topic = Topic, subpid = SubPid, subid = SubId, subopts = SubOpts}}, State) ->
    Subscriber = {SubPid, SubId},
    case ets:member(?SUBOPTION, {Topic, Subscriber}) of
        false ->
            Group = maps:get(share, SubOpts, undefined),
            true = do_subscribe(Group, Topic, Subscriber, SubOpts),
            emqx_shared_sub:subscribe(Group, Topic, SubPid),
            emqx_router:add_route(From, Topic, dest(Group)),
            {noreply, monitor_subscriber(Subscriber, State)};
        true ->
            gen_server:reply(From, ok),
            {noreply, State}
    end;

handle_cast({From, #unsubscribe{topic = Topic, subpid = SubPid, subid = SubId}}, State) ->
    Subscriber = {SubPid, SubId},
    case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
        [{_, SubOpts}] ->
            Group = maps:get(share, SubOpts, undefined),
            true = do_unsubscribe(Group, Topic, Subscriber),
            emqx_shared_sub:unsubscribe(Group, Topic, SubPid),
            case ets:member(?SUBSCRIBER, Topic) of
                false -> emqx_router:del_route(From, Topic, dest(Group));
                true  -> gen_server:reply(From, ok)
            end;
        [] -> gen_server:reply(From, ok)
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    emqx_logger:error("[Broker] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SubPid, Reason}, State = #state{submap = SubMap}) ->
    case maps:find(SubPid, SubMap) of
        {ok, SubIds} ->
            lists:foreach(fun(SubId) -> subscriber_down({SubPid, SubId}) end, SubIds),
            {noreply, demonitor_subscriber(SubPid, State)};
        error ->
            emqx_logger:error("unexpected 'DOWN': ~p, reason: ~p", [SubPid, Reason]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    emqx_logger:error("[Broker] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

do_subscribe(Group, Topic, Subscriber, SubOpts) ->
    ets:insert(?SUBSCRIPTION, {Subscriber, shared(Group, Topic)}),
    ets:insert(?SUBSCRIBER, {Topic, shared(Group, Subscriber)}),
    ets:insert(?SUBOPTION, {{Topic, Subscriber}, SubOpts}).

do_unsubscribe(Group, Topic, Subscriber) ->
    ets:delete_object(?SUBSCRIPTION, {Subscriber, shared(Group, Topic)}),
    ets:delete_object(?SUBSCRIBER, {Topic, shared(Group, Subscriber)}),
    ets:delete(?SUBOPTION, {Topic, Subscriber}).

subscriber_down(Subscriber) ->
    Topics = lists:map(fun({_, {share, _, Topic}}) ->
                           Topic;
                          ({_, Topic}) ->
                           Topic
                       end, ets:lookup(?SUBSCRIPTION, Subscriber)),
    lists:foreach(fun(Topic) ->
                      case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
                          [{_, SubOpts}] ->
                              Group = maps:get(share, SubOpts, undefined),
                              true = do_unsubscribe(Group, Topic, Subscriber),
                              ets:member(?SUBSCRIBER, Topic)
                                orelse emqx_router:del_route(Topic, dest(Group));
                          [] -> ok
                      end
                  end, Topics).

monitor_subscriber({SubPid, SubId}, State = #state{submap = SubMap, submon = SubMon}) ->
    UpFun = fun(SubIds) -> lists:usort([SubId|SubIds]) end,
    State#state{submap = maps:update_with(SubPid, UpFun, [SubId], SubMap),
                submon = emqx_pmon:monitor(SubPid, SubMon)}.

demonitor_subscriber(SubPid, State = #state{submap = SubMap, submon = SubMon}) ->
    State#state{submap = maps:remove(SubPid, SubMap),
                submon = emqx_pmon:demonitor(SubPid, SubMon)}.

dest(undefined) -> node();
dest(Group)     -> {Group, node()}.

shared(undefined, Name) -> Name;
shared(Group, Name)     -> {share, Group, Name}.

