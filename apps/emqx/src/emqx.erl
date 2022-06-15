%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-elvis([{elvis_style, god_modules, disable}]).

%% Start/Stop the application
-export([
    start/0,
    is_running/0,
    is_running/1,
    stop/0
]).

%% PubSub API
-export([
    subscribe/1,
    subscribe/2,
    subscribe/3,
    publish/1,
    unsubscribe/1
]).

%% PubSub management API
-export([
    topics/0,
    subscriptions/1,
    subscribers/1,
    subscribed/2
]).

%% Hooks API
-export([
    run_hook/2,
    run_fold_hook/3
]).

%% Configs APIs
-export([
    get_config/1,
    get_config/2,
    get_raw_config/1,
    get_raw_config/2,
    update_config/2,
    update_config/3,
    remove_config/1,
    remove_config/2,
    reset_config/2,
    data_dir/0,
    etc_file/1,
    cert_file/1,
    mutable_certs_dir/0
]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Bootstrap, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqx application
-spec start() -> {ok, list(atom())} | {error, term()}.
start() ->
    application:ensure_all_started(?APP).

%% @doc Stop emqx application.
-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(?APP).

%% @doc Is emqx running?
-spec is_running(node()) -> boolean().
is_running(Node) ->
    case emqx_proto_v1:is_running(Node) of
        {badrpc, _} -> false;
        Result -> Result
    end.

%% @doc Is emqx running on this node?
-spec is_running() -> boolean().
is_running() ->
    case whereis(?APP) of
        undefined -> false;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

-spec subscribe(emqx_types:topic() | string()) -> ok.
subscribe(Topic) ->
    emqx_broker:subscribe(iolist_to_binary(Topic)).

-spec subscribe(emqx_types:topic() | string(), emqx_types:subid() | emqx_types:subopts()) -> ok.
subscribe(Topic, SubId) when is_atom(SubId); is_binary(SubId) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), SubId);
subscribe(Topic, SubOpts) when is_map(SubOpts) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), SubOpts).

-spec subscribe(
    emqx_types:topic() | string(),
    emqx_types:subid() | pid(),
    emqx_types:subopts()
) -> ok.
subscribe(Topic, SubId, SubOpts) when (is_atom(SubId) orelse is_binary(SubId)), is_map(SubOpts) ->
    emqx_broker:subscribe(iolist_to_binary(Topic), SubId, SubOpts).

-spec publish(emqx_types:message()) -> emqx_types:publish_result().
publish(Msg) ->
    emqx_broker:publish(Msg).

-spec unsubscribe(emqx_types:topic() | string()) -> ok.
unsubscribe(Topic) ->
    emqx_broker:unsubscribe(iolist_to_binary(Topic)).

%%--------------------------------------------------------------------
%% PubSub management API
%%--------------------------------------------------------------------

-spec topics() -> list(emqx_types:topic()).
topics() -> emqx_router:topics().

-spec subscribers(emqx_types:topic() | string()) -> [pid()].
subscribers(Topic) ->
    emqx_broker:subscribers(iolist_to_binary(Topic)).

-spec subscriptions(pid()) -> [{emqx_types:topic(), emqx_types:subopts()}].
subscriptions(SubPid) when is_pid(SubPid) ->
    emqx_broker:subscriptions(SubPid).

-spec subscribed(pid() | emqx_types:subid(), emqx_types:topic() | string()) -> boolean().
subscribed(SubPid, Topic) when is_pid(SubPid) ->
    emqx_broker:subscribed(SubPid, iolist_to_binary(Topic));
subscribed(SubId, Topic) when is_atom(SubId); is_binary(SubId) ->
    emqx_broker:subscribed(SubId, iolist_to_binary(Topic)).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------
-spec run_hook(emqx_hooks:hookpoint(), list(any())) -> ok | stop.
run_hook(HookPoint, Args) ->
    emqx_hooks:run(HookPoint, Args).

-spec run_fold_hook(emqx_hooks:hookpoint(), list(any()), any()) -> any().
run_fold_hook(HookPoint, Args, Acc) ->
    emqx_hooks:run_fold(HookPoint, Args, Acc).

-spec get_config(emqx_map_lib:config_key_path()) -> term().
get_config(KeyPath) ->
    emqx_config:get(KeyPath).

-spec get_config(emqx_map_lib:config_key_path(), term()) -> term().
get_config(KeyPath, Default) ->
    emqx_config:get(KeyPath, Default).

-spec get_raw_config(emqx_map_lib:config_key_path()) -> term().
get_raw_config(KeyPath) ->
    emqx_config:get_raw(KeyPath).

-spec get_raw_config(emqx_map_lib:config_key_path(), term()) -> term().
get_raw_config(KeyPath, Default) ->
    emqx_config:get_raw(KeyPath, Default).

-spec update_config(emqx_map_lib:config_key_path(), emqx_config:update_request()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(KeyPath, UpdateReq) ->
    update_config(KeyPath, UpdateReq, #{}).

-spec update_config(
    emqx_map_lib:config_key_path(),
    emqx_config:update_request(),
    emqx_config:update_opts()
) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config([RootName | _] = KeyPath, UpdateReq, Opts) ->
    emqx_config_handler:update_config(
        emqx_config:get_schema_mod(RootName),
        KeyPath,
        {{update, UpdateReq}, Opts}
    ).

-spec remove_config(emqx_map_lib:config_key_path()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config(KeyPath) ->
    remove_config(KeyPath, #{}).

-spec remove_config(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config([RootName | _] = KeyPath, Opts) ->
    emqx_config_handler:update_config(
        emqx_config:get_schema_mod(RootName),
        KeyPath,
        {remove, Opts}
    ).

-spec reset_config(emqx_map_lib:config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset_config([RootName | _] = KeyPath, Opts) ->
    case emqx_config:get_default_value(KeyPath) of
        {ok, Default} ->
            emqx_config_handler:update_config(
                emqx_config:get_schema_mod(RootName),
                KeyPath,
                {{update, Default}, Opts}
            );
        {error, _} = Error ->
            Error
    end.

%% @doc Returns the data directory which is set at boot time.
data_dir() ->
    application:get_env(emqx, data_dir, "data").

%% @doc Returns the directory for user uploaded certificates.
mutable_certs_dir() ->
    filename:join([data_dir(), certs]).

%% @doc Returns the absolute path for a PEM certificate file
%% which is installed or provisioned by sysadmin in $EMQX_ETC_DIR/certs.
cert_file(SubPath) ->
    filename:join([etc_dir(), "certs", SubPath]).

%% @doc Returns the absolute path for a file in EMQX's etc dir.
%% i.e. for rpm and deb installation, it's /etc/emqx/
%% for other installation, it's <install_root>/etc/
etc_file(SubPath) ->
    filename:join([etc_dir(), SubPath]).

etc_dir() ->
    %% EMQX_ETC_DIR set by emqx boot script,
    %% if it's not set, then it must be test environment
    %% which should uses default path
    Env = os:getenv("EMQX_ETC_DIR"),
    case Env =:= "" orelse Env =:= false of
        true -> "etc";
        false -> Env
    end.
