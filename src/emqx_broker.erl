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
-export([subscribe/1, subscribe/2, subscribe/3]).
-export([unsubscribe/1, unsubscribe/2]).
-export([subscriber_down/1]).
-export([publish/1, safe_publish/1]).
-export([dispatch/2, dispatch/3]).
-export([subscriptions/1, subscribers/1, subscribed/2]).
-export([get_subopts/2, set_subopts/2]).
-export([topics/0]).
%% Stats fun
-export([stats_fun/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SHARD, 1024).
-define(TIMEOUT, 60000).
-define(BROKER, ?MODULE).

%% ETS tables
-define(SUBID, emqx_subid).
-define(SUBOPTION, emqx_suboption).
-define(SUBSCRIBER, emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).

%% Gards
-define(is_subid(Id), (is_binary(Id) orelse is_atom(Id))).

-spec(start_link(atom(), pos_integer()) -> emqx_types:startlink_ret()).
start_link(Pool, Id) ->
    _ = create_tabs(),
    gen_server:start_link({local, emqx_misc:proc_name(?BROKER, Id)}, ?MODULE, [Pool, Id], []).

%%------------------------------------------------------------------------------
%% Create tabs
%%------------------------------------------------------------------------------

create_tabs() ->
    TabOpts = [public, {read_concurrency, true}, {write_concurrency, true}],

    %% SubId: SubId -> SubPid1, SubPid2,...
    _ = emqx_tables:new(?SUBID, [bag | TabOpts]),
    %% SubOption: {SubPid, Topic} -> SubOption
    _ = emqx_tables:new(?SUBOPTION, [set | TabOpts]),

    %% Subscription: SubPid -> Topic1, Topic2, Topic3, ...
    %% duplicate_bag: o(1) insert
    _ = emqx_tables:new(?SUBSCRIPTION, [duplicate_bag | TabOpts]),

    %% Subscriber: Topic -> SubPid1, SubPid2, SubPid3, ...
    %% duplicate_bag: o(1) insert
    emqx_tables:new(?SUBSCRIBER, [duplicate_bag | TabOpts]).

%%------------------------------------------------------------------------------
%% Subscribe API
%%------------------------------------------------------------------------------

-spec(subscribe(emqx_topic:topic()) -> ok).
subscribe(Topic) when is_binary(Topic) ->
    subscribe(Topic, undefined).

-spec(subscribe(emqx_topic:topic(), emqx_types:subid() | emqx_types:subopts()) -> ok).
subscribe(Topic, SubId) when is_binary(Topic), ?is_subid(SubId) ->
    subscribe(Topic, SubId, #{});
subscribe(Topic, SubOpts) when is_binary(Topic), is_map(SubOpts) ->
    subscribe(Topic, undefined, SubOpts).

-spec(subscribe(emqx_topic:topic(), emqx_types:subid(), emqx_types:subopts()) -> ok).
subscribe(Topic, SubId, SubOpts) when is_binary(Topic), ?is_subid(SubId), is_map(SubOpts) ->
    SubPid = self(),
    case ets:member(?SUBOPTION, {SubPid, Topic}) of
        false ->
            ok = emqx_broker_helper:monitor(SubPid, SubId),
            Group = maps:get(share, SubOpts, undefined),
            %% true = ets:insert(?SUBID, {SubId, SubPid}),
            true = ets:insert(?SUBSCRIPTION, {SubPid, Topic}),
            %% SeqId = emqx_broker_helper:create_seq(Topic),
            true = ets:insert(?SUBSCRIBER, {Topic, shared(Group, SubPid)}),
            true = ets:insert(?SUBOPTION, {{SubPid, Topic}, SubOpts}),
            ok = emqx_shared_sub:subscribe(Group, Topic, SubPid),
            call(pick(Topic), {subscribe, Group, Topic});
        true -> ok
    end.

%%------------------------------------------------------------------------------
%% Unsubscribe API
%%------------------------------------------------------------------------------

-spec(unsubscribe(emqx_topic:topic()) -> ok).
unsubscribe(Topic) when is_binary(Topic) ->
    SubPid = self(),
    case ets:lookup(?SUBOPTION, {SubPid, Topic}) of
        [{_, SubOpts}] ->
            Group = maps:get(share, SubOpts, undefined),
            true = ets:delete_object(?SUBSCRIPTION, {SubPid, Topic}),
            true = ets:delete_object(?SUBSCRIBER, {Topic, shared(Group, SubPid)}),
            true = ets:delete(?SUBOPTION, {SubPid, Topic}),
            ok = emqx_shared_sub:unsubscribe(Group, Topic, SubPid),
            call(pick(Topic), {unsubscribe, Group, Topic});
        [] -> ok
    end.

-spec(unsubscribe(emqx_topic:topic(), emqx_types:subid()) -> ok).
unsubscribe(Topic, _SubId) when is_binary(Topic) ->
    unsubscribe(Topic).

%%------------------------------------------------------------------------------
%% Publish
%%------------------------------------------------------------------------------

-spec(publish(emqx_types:message()) -> emqx_types:deliver_results()).
publish(Msg) when is_record(Msg, message) ->
    _ = emqx_tracer:trace(publish, Msg),
    case emqx_hooks:run('message.publish', [], Msg) of
        {ok, Msg1 = #message{topic = Topic}} ->
            Delivery = route(aggre(emqx_router:match_routes(Topic)), delivery(Msg1)),
            Delivery#delivery.results;
        {stop, _} ->
            emqx_logger:warning("Stop publishing: ~s", [emqx_message:format(Msg)]),
            []
    end.

%% Called internally
-spec(safe_publish(emqx_types:message()) -> ok).
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
    #delivery{sender = self(), message = Msg, results = []}.

%%------------------------------------------------------------------------------
%% Route
%%------------------------------------------------------------------------------

route([], Delivery = #delivery{message = Msg}) ->
    emqx_hooks:run('message.dropped', [#{node => node()}, Msg]),
    inc_dropped_cnt(Msg#message.topic), Delivery;

route([{To, Node}], Delivery) when Node =:= node() ->
    dispatch(To, Delivery);

route([{To, Node}], Delivery = #delivery{results = Results}) when is_atom(Node) ->
    forward(Node, To, Delivery#delivery{results = [{route, Node, To}|Results]});

route([{To, Group}], Delivery) when is_tuple(Group); is_binary(Group) ->
    emqx_shared_sub:dispatch(Group, To, Delivery);

route(Routes, Delivery) ->
    lists:foldl(fun(Route, Acc) -> route([Route], Acc) end, Delivery, Routes).

aggre([]) ->
    [];
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
forward(Node, To, Delivery) ->
    %% rpc:call to ensure the delivery, but the latency:(
    case emqx_rpc:call(Node, ?BROKER, dispatch, [To, Delivery]) of
        {badrpc, Reason} ->
            emqx_logger:error("[Broker] Failed to forward msg to ~s: ~p", [Node, Reason]),
            Delivery;
        Delivery1 -> Delivery1
    end.

-spec(dispatch(emqx_topic:topic(), emqx_types:delivery()) -> emqx_types:delivery()).
dispatch(Topic, Delivery = #delivery{message = Msg, results = Results}) ->
    case subscribers(Topic) of
        [] ->
            emqx_hooks:run('message.dropped', [#{node => node()}, Msg]),
            inc_dropped_cnt(Topic),
            Delivery;
        [SubPid] -> %% optimize?
            dispatch(SubPid, Topic, Msg),
            Delivery#delivery{results = [{dispatch, Topic, 1}|Results]};
        SubPids ->
            Count = lists:foldl(fun(SubPid, Acc) ->
                                    dispatch(SubPid, Topic, Msg), Acc + 1
                                end, 0, SubPids),
            Delivery#delivery{results = [{dispatch, Topic, Count}|Results]}
    end.

dispatch(SubPid, Topic, Msg) when is_pid(SubPid) ->
    SubPid ! {dispatch, Topic, Msg},
    true;
%% TODO: how to optimize the share sub?
dispatch({share, _Group, _SubPid}, _Topic, _Msg) ->
    false.

inc_dropped_cnt(<<"$SYS/", _/binary>>) ->
    ok;
inc_dropped_cnt(_Topic) ->
    emqx_metrics:inc('messages/dropped').

-spec(subscribers(emqx_topic:topic()) -> [pid()]).
subscribers(Topic) ->
    safe_lookup_element(?SUBSCRIBER, Topic, []).

%%------------------------------------------------------------------------------
%% Subscriber is down
%%------------------------------------------------------------------------------

-spec(subscriber_down(pid()) -> true).
subscriber_down(SubPid) ->
    lists:foreach(
      fun(Sub = {_, Topic}) ->
          case ets:lookup(?SUBOPTION, Sub) of
              [{_, SubOpts}] ->
                  Group = maps:get(share, SubOpts, undefined),
                  true = ets:delete_object(?SUBSCRIBER, {Topic, shared(Group, SubPid)}),
                  true = ets:delete(?SUBOPTION, Sub),
                  gen_server:cast(pick(Topic), {unsubscribe, Group, Topic});
              [] -> ok
          end
      end, ets:lookup(?SUBSCRIPTION, SubPid)),
      ets:delete(?SUBSCRIPTION, SubPid).

%%------------------------------------------------------------------------------
%% Management APIs
%%------------------------------------------------------------------------------

-spec(subscriptions(pid() | emqx_types:subid())
      -> [{emqx_topic:topic(), emqx_types:subopts()}]).
subscriptions(SubPid) ->
    [{Topic, safe_lookup_element(?SUBOPTION, {SubPid, Topic}, #{})}
      || Topic <- safe_lookup_element(?SUBSCRIPTION, SubPid, [])].

-spec(subscribed(pid(), emqx_topic:topic()) -> boolean()).
subscribed(SubPid, Topic) when is_pid(SubPid) ->
    ets:member(?SUBOPTION, {SubPid, Topic});
subscribed(SubId, Topic) when ?is_subid(SubId) ->
    %%FIXME:... SubId -> SubPid
    ets:member(?SUBOPTION, {SubId, Topic}).

-spec(get_subopts(pid(), emqx_topic:topic()) -> emqx_types:subopts()).
get_subopts(SubPid, Topic) when is_pid(SubPid), is_binary(Topic) ->
    safe_lookup_element(?SUBOPTION, {SubPid, Topic}, #{}).

-spec(set_subopts(emqx_topic:topic(), emqx_types:subopts()) -> boolean()).
set_subopts(Topic, NewOpts) when is_binary(Topic), is_map(NewOpts) ->
    Sub = {self(), Topic},
    case ets:lookup(?SUBOPTION, Sub) of
        [{_, OldOpts}] ->
            ets:insert(?SUBOPTION, {Sub, maps:merge(OldOpts, NewOpts)});
        [] -> false
    end.

-spec(topics() -> [emqx_topic:topic()]).
topics() ->
    emqx_router:topics().

safe_lookup_element(Tab, Key, Def) ->
    try ets:lookup_element(Tab, Key, 2) catch error:badarg -> Def end.

%%------------------------------------------------------------------------------
%% Stats fun
%%------------------------------------------------------------------------------

stats_fun() ->
    safe_update_stats(?SUBSCRIBER, 'subscribers/count', 'subscribers/max'),
    safe_update_stats(?SUBSCRIPTION, 'subscriptions/count', 'subscriptions/max'),
    safe_update_stats(?SUBOPTION, 'suboptions/count', 'suboptions/max').

safe_update_stats(Tab, Stat, MaxStat) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

%%------------------------------------------------------------------------------
%% Pick and call
%%------------------------------------------------------------------------------

call(Broker, Req) ->
    gen_server:call(Broker, Req, ?TIMEOUT).

%% Pick a broker
pick(Topic) ->
    gproc_pool:pick_worker(broker, Topic).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({subscribe, Group, Topic}, _From, State) ->
    Ok = emqx_router:add_route(Topic, dest(Group)),
    {reply, Ok, State};

handle_call({unsubscribe, Group, Topic}, _From, State) ->
    Ok = case ets:member(?SUBSCRIBER, Topic) of
             false -> emqx_router:delete_route(Topic, dest(Group));
             true  -> ok
         end,
    {reply, Ok, State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[Broker] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Broker] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[Broker] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

dest(undefined) -> node();
dest(Group)     -> {Group, node()}.

shared(undefined, Name) -> Name;
shared(Group, Name)     -> {share, Group, Name}.

