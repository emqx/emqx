%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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


%% Start/Stop the application
-export([ start/0
        , is_running/1
        , stop/0
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

%% Troubleshooting
-export([ set_debug_secret/1
        ]).

-export([ update_config/2
        , update_config/3
        , remove_config/1
        , remove_config/2
        , reset_config/2
        ]).

-define(APP, ?MODULE).

%% @hidden Path to the file which has debug_info encryption secret in it.
%% Evaluate this function if there is a need to access encrypted debug_info.
%% NOTE: Do not change the API to accept the secret text because it may
%% get logged everywhere.
set_debug_secret(PathToSecretFile) ->
    SecretText =
        case file:read_file(PathToSecretFile) of
            {ok, Secret} ->
                try string:trim(binary_to_list(Secret))
                catch _ : _ -> error({badfile, PathToSecretFile})
                end;
            {error, Reason} ->
                ?ULOG("Failed to read debug_info encryption key file ~s: ~p~n",
                      [PathToSecretFile, Reason]),
                error(Reason)
        end,
    F = fun(init) -> ok;
           (clear) -> ok;
           ({debug_info, _Mode, _Module, _Filename}) -> SecretText
        end,
    _ = beam_lib:clear_crypto_key_fun(),
    ok = beam_lib:crypto_key_fun(F).

%%--------------------------------------------------------------------
%% Bootstrap, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqx application
-spec(start() -> {ok, list(atom())} | {error, term()}).
start() ->
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

-spec(subscribers(emqx_topic:topic() | string()) -> [pid()]).
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

-spec(hook(emqx_hooks:hookpoint(),
           emqx_hooks:action(),
           emqx_hooks:filter() | integer() | list())
      -> ok | {error, already_exists}).
hook(HookPoint, Action, Priority) when is_integer(Priority) ->
    emqx_hooks:add(HookPoint, Action, Priority);
hook(HookPoint, Action, {_M, _F, _A} = Filter ) ->
    emqx_hooks:add(HookPoint, Action, Filter).

-spec(hook(emqx_hooks:hookpoint(), emqx_hooks:action(), emqx_hooks:filter(), integer())
      -> ok | {error, already_exists}).
hook(HookPoint, Action, Filter, Priority) ->
    emqx_hooks:add(HookPoint, Action, Filter, Priority).

-spec(unhook(emqx_hooks:hookpoint(), emqx_hooks:action() | {module(), atom()}) -> ok).
unhook(HookPoint, Action) ->
    emqx_hooks:del(HookPoint, Action).

-spec(run_hook(emqx_hooks:hookpoint(), list(any())) -> ok | stop).
run_hook(HookPoint, Args) ->
    emqx_hooks:run(HookPoint, Args).

-spec(run_fold_hook(emqx_hooks:hookpoint(), list(any()), any()) -> any()).
run_fold_hook(HookPoint, Args, Acc) ->
    emqx_hooks:run_fold(HookPoint, Args, Acc).

-spec update_config(emqx_map_lib:config_key_path(), emqx_config:update_request()) ->
    {ok, emqx_config:config(), emqx_config:raw_config()} | {error, term()}.
update_config(KeyPath, UpdateReq) ->
    update_config(KeyPath, UpdateReq, #{}).

-spec update_config(emqx_map_lib:config_key_path(), emqx_config:update_request(),
             emqx_config:update_opts()) ->
    {ok, emqx_config:config(), emqx_config:raw_config()} | {error, term()}.
update_config([RootName | _] = KeyPath, UpdateReq, Opts) ->
    emqx_config_handler:update_config(emqx_config:get_schema_mod(RootName), KeyPath,
        {{update, UpdateReq}, Opts}).

-spec remove_config(emqx_map_lib:config_key_path()) ->
    {ok, emqx_config:config(), emqx_config:raw_config()} | {error, term()}.
remove_config(KeyPath) ->
    remove_config(KeyPath, #{}).

-spec remove_config(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    ok | {error, term()}.
remove_config([RootName | _] = KeyPath, Opts) ->
    emqx_config_handler:update_config(emqx_config:get_schema_mod(RootName),
        KeyPath, {remove, Opts}).

-spec reset_config(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:config(), emqx_config:raw_config()} | {error, term()}.
reset_config([RootName | _] = KeyPath, Opts) ->
    case emqx_config:get_default_value(KeyPath) of
        {ok, Default} ->
            emqx_config_handler:update_config(emqx_config:get_schema_mod(RootName), KeyPath,
                {{update, Default}, Opts});
        {error, _} = Error ->
            Error
    end.
