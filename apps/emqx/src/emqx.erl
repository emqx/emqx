%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx).

-include("emqx.hrl").
-include("emqx_config.hrl").
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

%% Cluster API
-export([
    cluster_nodes/1,
    running_nodes/0
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

%% Configs APIs
-export([
    get_config/1,
    get_config/2,
    get_raw_config/1,
    get_raw_config/2,
    update_config/2,
    update_config/3,
    update_config/4,
    remove_config/1,
    remove_config/2,
    remove_config/3,
    reset_config/2,
    reset_config/3,
    data_dir/0,
    etc_file/1,
    cert_file/1,
    mutable_certs_dir/0
]).

-define(APP, ?MODULE).

-type config_key_path() :: emqx_utils_maps:config_key_path().
-type maybe_ns_config_key_path() :: config_key_path() | {namespace(), config_key_path()}.
-type namespace() :: binary().

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
%% Cluster API
%%--------------------------------------------------------------------

-spec running_nodes() -> [node()].
running_nodes() ->
    mria:running_nodes().

-spec cluster_nodes(all | running | cores | stopped) -> [node()].
cluster_nodes(Type) ->
    mria:cluster_nodes(Type).

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
%% Config API
%%--------------------------------------------------------------------

-spec get_config(maybe_ns_config_key_path()) -> term().
get_config({?global_ns, KeyPath}) ->
    get_config(KeyPath);
get_config({Namespace, KeyPath}) ->
    KeyPath1 = emqx_config:ensure_atom_conf_path(KeyPath, {raise_error, config_not_found}),
    emqx_config:get_namespaced(KeyPath1, Namespace);
get_config(KeyPath) ->
    KeyPath1 = emqx_config:ensure_atom_conf_path(KeyPath, {raise_error, config_not_found}),
    emqx_config:get(KeyPath1).

-spec get_config(maybe_ns_config_key_path(), term()) -> term().
get_config({?global_ns, KeyPath}, Default) ->
    get_config(KeyPath, Default);
get_config({Namespace, KeyPath}, Default) ->
    try
        KeyPath1 = emqx_config:ensure_atom_conf_path(KeyPath, {raise_error, config_not_found}),
        emqx_config:get_namespaced(KeyPath1, Namespace, Default)
    catch
        error:config_not_found ->
            Default
    end;
get_config(KeyPath, Default) ->
    try
        KeyPath1 = emqx_config:ensure_atom_conf_path(KeyPath, {raise_error, config_not_found}),
        emqx_config:get(KeyPath1, Default)
    catch
        error:config_not_found ->
            Default
    end.

-spec get_raw_config(maybe_ns_config_key_path()) -> term().
get_raw_config({?global_ns, KeyPath}) ->
    get_raw_config(KeyPath);
get_raw_config({Namespace, KeyPath}) ->
    emqx_config:get_raw_namespaced(KeyPath, Namespace);
get_raw_config(KeyPath) ->
    emqx_config:get_raw(KeyPath).

-spec get_raw_config(maybe_ns_config_key_path(), term()) -> term().
get_raw_config({?global_ns, KeyPath}, Default) ->
    get_raw_config(KeyPath, Default);
get_raw_config({Namespace, KeyPath}, Default) ->
    emqx_config:get_raw_namespaced(KeyPath, Namespace, Default);
get_raw_config(KeyPath, Default) ->
    emqx_config:get_raw(KeyPath, Default).

-spec update_config(config_key_path(), emqx_config:update_request()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(KeyPath, UpdateReq) ->
    update_config(KeyPath, UpdateReq, #{}, #{}).

-spec update_config(
    config_key_path(),
    emqx_config:update_request(),
    emqx_config:update_opts()
) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(KeyPath, UpdateReq, Opts) ->
    update_config(KeyPath, UpdateReq, Opts, #{}).

-spec update_config(
    config_key_path(),
    emqx_config:update_request(),
    emqx_config:update_opts(),
    emqx_config:cluster_rpc_opts()
) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config([RootName | _] = KeyPath, UpdateReq, Opts, ClusterRpcOpts) ->
    Mod = emqx_config:get_schema_mod(RootName),
    emqx_config_handler:update_config(
        Mod,
        KeyPath,
        {{update, UpdateReq}, Opts},
        ClusterRpcOpts
    ).

-spec remove_config(config_key_path()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config(KeyPath) ->
    remove_config(KeyPath, #{}, #{}).

-spec remove_config(config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config([_RootName | _] = KeyPath, Opts) ->
    remove_config(KeyPath, Opts, #{}).

-spec remove_config(
    config_key_path(), emqx_config:update_opts(), emqx_config:cluster_rpc_opts()
) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config([RootName | _] = KeyPath, Opts, ClusterRpcOpts) ->
    emqx_config_handler:update_config(
        emqx_config:get_schema_mod(RootName),
        KeyPath,
        {remove, Opts},
        ClusterRpcOpts
    ).

-spec reset_config(config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset_config([RootName | SubKeys] = KeyPath, Opts) ->
    reset_config([RootName | SubKeys] = KeyPath, Opts, #{}).

-spec reset_config(
    config_key_path(), emqx_config:update_opts(), emqx_config:cluster_rpc_opts()
) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset_config([RootName | SubKeys] = KeyPath, Opts, ClusterRpcOpts) ->
    case emqx_config:get_default_value(KeyPath) of
        {ok, Default} ->
            Mod = emqx_config:get_schema_mod(RootName),
            case SubKeys =:= [] of
                true ->
                    emqx_config_handler:update_config(
                        Mod,
                        KeyPath,
                        {{update, Default}, Opts},
                        ClusterRpcOpts
                    );
                false ->
                    NewConf =
                        emqx_utils_maps:deep_put(
                            SubKeys,
                            emqx_config:get_raw([RootName], #{}),
                            Default
                        ),
                    emqx_config_handler:update_config(
                        Mod,
                        [RootName],
                        {{update, NewConf}, Opts},
                        ClusterRpcOpts
                    )
            end;
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
