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

-module(emqx).

-include("emqx.hrl").

%% Start/Stop the application
-export([start/0, is_running/1, stop/0]).

%% PubSub API
-export([subscribe/1, subscribe/2, subscribe/3]).
-export([publish/1]).
-export([unsubscribe/1, unsubscribe/2]).

%% PubSub management API
-export([topics/0, subscriptions/1, subscribers/1, subscribed/2]).
-export([get_subopts/2, set_subopts/3]).

%% Hooks API
-export([hook/4, hook/3, unhook/2, run_hooks/2, run_hooks/3]).

%% Shutdown and reboot
-export([shutdown/0, shutdown/1, reboot/0]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Bootstrap, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqx application
-spec(start() -> {ok, list(atom())} | {error, term()}).
start() ->
    %% Check OS
    %% Check VM
    %% Check Mnesia
    application:ensure_all_started(?APP).

%% @doc Stop emqx application.
-spec(stop() -> ok | {error, term()}).
stop() ->
    application:stop(?APP).

%% @doc Is emqx running?
-spec(is_running(node()) -> boolean()).
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [?APP]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

-spec(subscribe(topic() | string()) -> ok | {error, term()}).
subscribe(Topic) ->
    emqx_broker:subscribe(iolist_to_binary(Topic)).

-spec(subscribe(topic() | string(), subscriber() | string()) -> ok | {error, term()}).
subscribe(Topic, Sub) when is_list(Sub)->
    emqx_broker:subscribe(iolist_to_binary(Topic), list_to_subid(Sub));
subscribe(Topic, Subscriber) when is_tuple(Subscriber) ->
    {SubPid, SubId} = Subscriber,
    emqx_broker:subscribe(iolist_to_binary(Topic), SubPid, SubId).

-spec(subscribe(topic() | string(), subscriber() | string(), subopts()) -> ok | {error, term()}).
subscribe(Topic, Sub, Options) when is_list(Sub)->
    emqx_broker:subscribe(iolist_to_binary(Topic), list_to_subid(Sub), Options);
subscribe(Topic, Subscriber, Options) when is_tuple(Subscriber)->
    {SubPid, SubId} = Subscriber,
    emqx_broker:subscribe(iolist_to_binary(Topic), SubPid, SubId, Options).

%% @doc Publish Message
-spec(publish(message()) -> {ok, delivery()} | {error, term()}).
publish(Msg) ->
    emqx_broker:publish(Msg).

-spec(unsubscribe(topic() | string()) -> ok | {error, term()}).
unsubscribe(Topic) ->
    emqx_broker:unsubscribe(iolist_to_binary(Topic)).

-spec(unsubscribe(topic() | string(), subscriber() | string()) -> ok | {error, term()}).
unsubscribe(Topic, Subscriber) ->
    emqx_broker:unsubscribe(iolist_to_binary(Topic), list_to_subid(Subscriber)).

%%--------------------------------------------------------------------
%% PubSub management API
%%--------------------------------------------------------------------

-spec(get_subopts(topic() | string(), subscriber()) -> subopts()).
get_subopts(Topic, Subscriber) ->
    emqx_broker:get_subopts(iolist_to_binary(Topic), list_to_subid(Subscriber)).

-spec(set_subopts(topic() | string(), subscriber(), subopts()) -> ok).
set_subopts(Topic, Subscriber, Options) when is_list(Options) ->
    emqx_broker:set_subopts(iolist_to_binary(Topic), list_to_subid(Subscriber), Options).

-spec(topics() -> list(topic())).
topics() -> emqx_router:topics().

-spec(subscribers(topic() | string()) -> list(subscriber())).
subscribers(Topic) ->
    emqx_broker:subscribers(iolist_to_binary(Topic)).

-spec(subscriptions(subscriber()) -> [{topic(), subopts()}]).
subscriptions(Subscriber) ->
    emqx_broker:subscriptions(Subscriber).

-spec(subscribed(topic() | string(), subscriber()) -> boolean()).
subscribed(Topic, Subscriber) ->
    emqx_broker:subscribed(iolist_to_binary(Topic), list_to_subid(Subscriber)).

list_to_subid(SubId) when is_binary(SubId) ->
    SubId;
list_to_subid(SubId) when is_list(SubId) ->
    iolist_to_binary(SubId);
list_to_subid(SubPid) when is_pid(SubPid) ->
    SubPid.

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

-spec(hook(atom(), function() | {emqx_hooks:hooktag(), function()}, list(any()))
      -> ok | {error, term()}).
hook(Hook, TagFunction, InitArgs) ->
    emqx_hooks:add(Hook, TagFunction, InitArgs).

-spec(hook(atom(), function() | {emqx_hooks:hooktag(), function()}, list(any()), integer())
      -> ok | {error, term()}).
hook(Hook, TagFunction, InitArgs, Priority) ->
    emqx_hooks:add(Hook, TagFunction, InitArgs, Priority).

-spec(unhook(atom(), function() | {emqx_hooks:hooktag(), function()})
      -> ok | {error, term()}).
unhook(Hook, TagFunction) ->
    emqx_hooks:delete(Hook, TagFunction).

-spec(run_hooks(atom(), list(any())) -> ok | stop).
run_hooks(Hook, Args) ->
    emqx_hooks:run(Hook, Args).

-spec(run_hooks(atom(), list(any()), any()) -> {ok | stop, any()}).
run_hooks(Hook, Args, Acc) ->
    emqx_hooks:run(Hook, Args, Acc).

%%--------------------------------------------------------------------
%% Shutdown and reboot
%%--------------------------------------------------------------------

shutdown() ->
    shutdown(normal).

shutdown(Reason) ->
    emqx_logger:error("emqx shutdown for ~s", [Reason]),
    emqx_plugins:unload(),
    lists:foreach(fun application:stop/1, [emqx, ekka, cowboy, esockd, gproc]).

reboot() ->
    lists:foreach(fun application:start/1, [gproc, esockd, cowboy, ekka, emqx]).

