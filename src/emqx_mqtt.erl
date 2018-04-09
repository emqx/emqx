%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_mqtt).

-include("emqx_mqtt.hrl").

-export([bootstrap/0, shutdown/0]).

-export([start_listeners/0, start_listener/1]).
-export([stop_listeners/0, stop_listener/1]).
-export([restart_listeners/0, restart_listener/1]).
-export([listeners/0]).

-type(listener() :: {atom(), esockd:listen_on(), [esockd:option()]}).

bootstrap() ->
    start_listeners().

shutdown() ->
    stop_listeners().

%%--------------------------------------------------------------------
%% Start/Stop Listeners
%%--------------------------------------------------------------------

%% @doc Start Listeners.
-spec(start_listeners() -> ok).
start_listeners() ->
    lists:foreach(fun start_listener/1, emqx_conf:get_env(listeners, [])).

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
    Env = lists:append(emqx_conf:get_env(client, []), emqx_conf:get_env(protocol, [])),
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
stop_listeners() ->
    lists:foreach(fun stop_listener/1, emqx_conf:get_env(listeners, [])).

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
restart_listeners() ->
    lists:foreach(fun restart_listener/1,
                  emqx_conf:get_env(listeners, [])).

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

