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

-module(emqx).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[EMQ X]").

%% Start/Stop the application
-export([ start/0
        , restart/1
        , is_running/1
        , stop/0
        ]).

-export([ get_env/1
        , get_env/2
        ]).

%% PubSub API
-export([ subscribe/1
        , subscribe/2
        , subscribe/3
        , publish/1
        , unsubscribe/1
        ]).

%% PubSub management API
-export([ topics/0
        , subscriptions/1
        , subscribers/1
        , subscribed/2
        ]).

%% Hooks API
-export([ hook/2
        , hook/3
        , hook/4
        , unhook/2
        , run_hook/2
        , run_fold_hook/3
        ]).

%% Shutdown and reboot
-export([ shutdown/0
        , shutdown/1
        , reboot/0
        ]).

-define(APP, ?MODULE).

-define(COPYRIGHT, "Copyright (c) 2020 EMQ Technologies Co., Ltd").

-define(LICENSE_MESSAGE, "Licensed under the Apache License, Version 2.0").

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

-spec(restart(string()) -> ok).
restart(ConfFile) ->
    reload_config(ConfFile),
    shutdown(),
    ok = application:stop(mnesia),
    application:start(mnesia),
    reboot().

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

%% @doc Get environment
-spec(get_env(Key :: atom()) -> maybe(term())).
get_env(Key) ->
    get_env(Key, undefined).

-spec(get_env(Key :: atom(), Default :: term()) -> term()).
get_env(Key, Default) ->
    application:get_env(?APP, Key, Default).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

-spec(subscribe(emqx_topic:topic() | string()) -> ok).
subscribe(Topic) ->
    emqx_broker:subscribe(iolist_to_binary(Topic)).

-spec(subscribe(emqx_topic:topic() | string(), emqx_types:subid() | emqx_types:subopts()) -> ok).
subscribe(Topic, SubId) when is_atom(SubId); is_binary(SubId)->
    emqx_broker:subscribe(iolist_to_binary(Topic), SubId);
subscribe(Topic, SubOpts) when is_map(SubOpts) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), SubOpts).

-spec(subscribe(emqx_topic:topic() | string(),
                emqx_types:subid() | pid(), emqx_types:subopts()) -> ok).
subscribe(Topic, SubId, SubOpts) when (is_atom(SubId) orelse is_binary(SubId)), is_map(SubOpts) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), SubId, SubOpts).

-spec(publish(emqx_types:message()) -> emqx_types:publish_result()).
publish(Msg) ->
    emqx_broker:publish(Msg).

-spec(unsubscribe(emqx_topic:topic() | string()) -> ok).
unsubscribe(Topic) ->
    emqx_broker:unsubscribe(iolist_to_binary(Topic)).

%%--------------------------------------------------------------------
%% PubSub management API
%%--------------------------------------------------------------------

-spec(topics() -> list(emqx_topic:topic())).
topics() -> emqx_router:topics().

-spec(subscribers(emqx_topic:topic() | string()) -> list(emqx_types:subscriber())).
subscribers(Topic) ->
    emqx_broker:subscribers(iolist_to_binary(Topic)).

-spec(subscriptions(pid()) -> [{emqx_topic:topic(), emqx_types:subopts()}]).
subscriptions(SubPid) when is_pid(SubPid) ->
    emqx_broker:subscriptions(SubPid).

-spec(subscribed(pid() | emqx_types:subid(), emqx_topic:topic() | string()) -> boolean()).
subscribed(SubPid, Topic) when is_pid(SubPid) ->
    emqx_broker:subscribed(SubPid, iolist_to_binary(Topic));
subscribed(SubId, Topic) when is_atom(SubId); is_binary(SubId) ->
    emqx_broker:subscribed(SubId, iolist_to_binary(Topic)).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

-spec(hook(emqx_hooks:hookpoint(), emqx_hooks:action()) -> ok | {error, already_exists}).
hook(HookPoint, Action) ->
    emqx_hooks:add(HookPoint, Action).

-spec(hook(emqx_hooks:hookpoint(), emqx_hooks:action(), emqx_hooks:filter() | integer())
      -> ok | {error, already_exists}).
hook(HookPoint, Action, Priority) when is_integer(Priority) ->
    emqx_hooks:add(HookPoint, Action, Priority);
hook(HookPoint, Action, Filter) when is_function(Filter); is_tuple(Filter) ->
    emqx_hooks:add(HookPoint, Action, Filter);
hook(HookPoint, Action, InitArgs) when is_list(InitArgs) ->
    emqx_hooks:add(HookPoint, Action, InitArgs).

-spec(hook(emqx_hooks:hookpoint(), emqx_hooks:action(), emqx_hooks:filter(), integer())
      -> ok | {error, already_exists}).
hook(HookPoint, Action, Filter, Priority) ->
    emqx_hooks:add(HookPoint, Action, Filter, Priority).

-spec(unhook(emqx_hooks:hookpoint(), emqx_hooks:action()) -> ok).
unhook(HookPoint, Action) ->
    emqx_hooks:del(HookPoint, Action).

-spec(run_hook(emqx_hooks:hookpoint(), list(any())) -> ok | stop).
run_hook(HookPoint, Args) ->
    emqx_hooks:run(HookPoint, Args).

-spec(run_fold_hook(emqx_hooks:hookpoint(), list(any()), any()) -> any()).
run_fold_hook(HookPoint, Args, Acc) ->
    emqx_hooks:run_fold(HookPoint, Args, Acc).

%%--------------------------------------------------------------------
%% Shutdown and reboot
%%--------------------------------------------------------------------

shutdown() ->
    shutdown(normal).

shutdown(Reason) ->
    ?LOG(critical, "emqx shutdown for ~s", [Reason]),
    emqx_alarm_handler:unload(),
    emqx_plugins:unload(),
    lists:foreach(fun application:stop/1, [emqx, ekka, cowboy, ranch, esockd, gproc]).

reboot() ->
    lists:foreach(fun application:start/1, [gproc, esockd, ranch, cowboy, ekka, emqx]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

reload_config(ConfFile) ->
    {ok, [Conf]} = file:consult(ConfFile),
    lists:foreach(fun({App, Vals}) ->
                      [application:set_env(App, Par, Val) || {Par, Val} <- Vals]
                  end, Conf).

