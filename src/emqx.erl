%%--------------------------------------------------------------------
%% Copyright (C) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

%% Start/Stop Application
-export([start/0, env/1, env/2, is_running/1, stop/0]).

%% Start/Stop Listeners
-export([start_listeners/0, start_listener/1, listeners/0,
         stop_listeners/0, stop_listener/1,
         restart_listeners/0, restart_listener/1]).

%% PubSub API
-export([subscribe/1, subscribe/2, subscribe/3, publish/1,
         unsubscribe/1, unsubscribe/2]).

%% PubSub management API
-export([topics/0, subscriptions/1, subscribers/1, subscribed/2]).

%% Get/Set suboptions
-export([getopts/2, setopts/3]).

%% Hooks API
-export([hook/4, hook/3, unhook/2, run_hooks/2, run_hooks/3]).

%% Debug API
-export([dump/0]).

%% Shutdown and reboot
-export([shutdown/0, shutdown/1, reboot/0]).

-type(listener() :: {atom(), esockd:listen_on(), [esockd:option()]}).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Bootstrap, environment, configuration, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqx application
-spec(start() -> ok | {error, term()}).
start() -> application:start(?APP).

%% @doc Stop emqx application.
-spec(stop() -> ok | {error, term()}).
stop() -> application:stop(?APP).

%% @doc Get environment
-spec(env(Key :: atom()) -> {ok, any()} | undefined).
env(Key) -> application:get_env(?APP, Key).

%% @doc Get environment with default
-spec(env(Key :: atom(), Default :: any()) -> undefined | any()).
env(Key, Default) -> application:get_env(?APP, Key, Default).

%% @doc Is emqx running?
-spec(is_running(node()) -> boolean()).
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [?APP]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

%% @doc Start Listeners.
-spec(start_listeners() -> ok).
start_listeners() -> lists:foreach(fun start_listener/1, env(listeners, [])).

%% Start mqtt listener
-spec(start_listener(listener()) -> {ok, pid()} | {error, any()}).
start_listener({tcp, ListenOn, Opts}) ->
    start_listener('mqtt:tcp', ListenOn, Opts);

%% Start mqtt(SSL) listener
start_listener({ssl, ListenOn, Opts}) ->
    start_listener('mqtt:ssl', ListenOn, Opts);

%% Start http listener
start_listener({Proto, ListenOn, Opts}) when Proto == http; Proto == ws ->
    {ok, _} = mochiweb:start_http('mqtt:ws', ListenOn, Opts, {emqx_ws, handle_request, []});

%% Start https listener
start_listener({Proto, ListenOn, Opts}) when Proto == https; Proto == wss ->
    {ok, _} = mochiweb:start_http('mqtt:wss', ListenOn, Opts, {emqx_ws, handle_request, []}).

start_listener(Proto, ListenOn, Opts) ->
    Env = lists:append(emqx:env(client, []), emqx:env(protocol, [])),
    MFArgs = {emqx_connection, start_link, [Env]},
    {ok, _} = esockd:open(Proto, ListenOn, merge_sockopts(Opts), MFArgs).

listeners() ->
    [Listener || Listener = {{Proto, _}, _Pid} <- esockd:listeners(), is_mqtt(Proto)].

is_mqtt('mqtt:tcp') -> true;
is_mqtt('mqtt:ssl') -> true;
is_mqtt('mqtt:ws')  -> true;
is_mqtt('mqtt:wss') -> true;
is_mqtt(_Proto)     -> false.

%% @doc Stop Listeners
-spec(stop_listeners() -> ok).
stop_listeners() -> lists:foreach(fun stop_listener/1, env(listeners, [])).

-spec(stop_listener(listener()) -> ok | {error, any()}).
stop_listener({tcp, ListenOn, _Opts}) ->
    esockd:close('mqtt:tcp', ListenOn);
stop_listener({ssl, ListenOn, _Opts}) ->
    esockd:close('mqtt:ssl', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == http; Proto == ws ->
    mochiweb:stop_http('mqtt:ws', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) when Proto == https; Proto == wss ->
    mochiweb:stop_http('mqtt:wss', ListenOn);
% stop_listener({Proto, ListenOn, _Opts}) when Proto == api ->
%     mochiweb:stop_http('mqtt:api', ListenOn);
stop_listener({Proto, ListenOn, _Opts}) ->
    esockd:close(Proto, ListenOn).

%% @doc Restart Listeners
-spec(restart_listeners() -> ok).
restart_listeners() -> lists:foreach(fun restart_listener/1, env(listeners, [])).

-spec(restart_listener(listener()) -> any()).
restart_listener({tcp, ListenOn, _Opts}) ->
    esockd:reopen('mqtt:tcp', ListenOn);
restart_listener({ssl, ListenOn, _Opts}) ->
    esockd:reopen('mqtt:ssl', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == http; Proto == ws ->
    mochiweb:restart_http('mqtt:ws', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == https; Proto == wss ->
    mochiweb:restart_http('mqtt:wss', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) when Proto == api ->
    mochiweb:restart_http('mqtt:api', ListenOn);
restart_listener({Proto, ListenOn, _Opts}) ->
    esockd:reopen(Proto, ListenOn).

merge_sockopts(Options) ->
    SockOpts = emqx_misc:merge_opts(
                 ?MQTT_SOCKOPTS, proplists:get_value(sockopts, Options, [])),
    emqx_misc:merge_opts(Options, [{sockopts, SockOpts}]).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

-spec(subscribe(topic() | string()) -> ok | {error, term()}).
subscribe(Topic) ->
    emqx_broker:subscribe(iolist_to_binary(Topic)).

-spec(subscribe(topic() | iodata(), subscriber() | string()) -> ok | {error, term()}).
subscribe(Topic, Subscriber) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), list_to_subid(Subscriber)).

-spec(subscribe(topic() | iodata(), subscriber() | string(), [suboption()]) -> ok | {error, term()}).
subscribe(Topic, Subscriber, Options) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), list_to_subid(Subscriber), Options).

%% @doc Publish Message
-spec(publish(message()) -> {ok, delivery()} | ignore).
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

-spec(getopts(topic() | string(), subscriber()) -> [suboption()]).
getopts(Topic, Subscriber) ->
    emqx_broker:getopts(iolist_to_binary(Topic), list_to_subid(Subscriber)).

-spec(setopts(topic() | string(), subscriber(), [suboption()]) -> ok).
setopts(Topic, Subscriber, Options) when is_list(Options) ->
    emqx_broker:setopts(iolist_to_binary(Topic), list_to_subid(Subscriber), Options).

-spec(topics() -> list(topic())).
topics() -> emqx_router:topics().

-spec(subscribers(topic() | string()) -> list(subscriber())).
subscribers(Topic) ->
    emqx_broker:subscribers(iolist_to_binary(Topic)).

-spec(subscriptions(subscriber() | string()) -> [{topic(), list(suboption())}]).
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
    SubPid;
list_to_subid({SubId, SubPid}) when is_binary(SubId), is_pid(SubPid) ->
    {SubId, SubPid};
list_to_subid({SubId, SubPid}) when is_list(SubId), is_pid(SubPid) ->
    {iolist_to_binary(SubId), SubPid}.

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
    emqx_log:error("EMQ shutdown for ~s", [Reason]),
    emqx_plugins:unload(),
    lists:foreach(fun application:stop/1, [emqx, ekka, mochiweb, esockd, gproc]).

reboot() ->
    lists:foreach(fun application:start/1, [gproc, esockd, mochiweb, ekka, emqx]).

%%--------------------------------------------------------------------
%% Debug
%%--------------------------------------------------------------------

dump() -> lists:append([emqx_broker:dump(), emqx_router:dump()]).

