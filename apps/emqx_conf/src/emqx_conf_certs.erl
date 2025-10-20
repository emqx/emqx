%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_conf_certs).

%% API
-export([
    list_managed_files/2,
    list_bundles/1,
    delete_bundle/2,
    add_managed_file/4
]).

%% RPC targets (v1)
-export([
    add_managed_file_v1/4,
    delete_managed_file_v1/3,
    delete_bundle_v1/2
]).

-export_type([
    file_kind/0,
    bundle_name/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_conf_certs.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(path, path).

-define(FILENAME_KEY, "key.pem").
-define(FILENAME_CHAIN, "chain.pem").
-define(FILENAME_CA, "ca.pem").
-define(FILENAME_ACC_KEY, "acc-key.pem").
-define(FILENAME_KEY_PASSWORD, "key-password").

-define(BPAPI, emqx_mgmt_api_certs).

-type maybe_namespace() :: emqx_config:maybe_namespace().
-type file_kind() ::
    ?FILE_KIND_KEY
    | ?FILE_KIND_CHAIN
    | ?FILE_KIND_CA
    | ?FILE_KIND_ACC_KEY.
-type bundle_name() :: binary().
-type contents() :: binary().
-type managed_file() :: #{
    ?path := file:filename()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec list_managed_files(maybe_namespace(), bundle_name()) ->
    {ok, #{file_kind() => managed_file()}} | {error, file:posix()}.
list_managed_files(Namespace, BundleName) ->
    Dir = dir(Namespace, BundleName),
    maybe
        {ok, Files0} ?= file:list_dir(Dir),
        Files = lists:foldl(
            fun(Filename, Acc) ->
                maybe
                    {ok, Kind} ?= filename_to_kind(Filename),
                    Path = filename:join([Dir, Filename]),
                    true ?= filelib:is_regular(Path),
                    Acc#{Kind => #{?path => Path}}
                else
                    _ -> Acc
                end
            end,
            #{},
            Files0
        ),
        {ok, Files}
    end.

-spec list_bundles(maybe_namespace()) ->
    {ok, [bundle_name()]} | {error, file:posix()}.
list_bundles(Namespace) ->
    Dir = base_dir(Namespace),
    maybe
        {ok, Contents} ?= file:list_dir(Dir),
        Bundles = lists:sort(
            lists:filter(
                fun(Filename) ->
                    filelib:is_dir(filename:join([Dir, Filename]))
                end,
                Contents
            )
        ),
        {ok, Bundles}
    else
        {error, enoent} ->
            {ok, []};
        Error ->
            Error
    end.

-spec delete_bundle(maybe_namespace(), bundle_name()) ->
    ok | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
delete_bundle(Namespace, BundleName) ->
    Nodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI, 1),
    Res = emqx_mgmt_api_certs_proto_v1:delete_bundle(Nodes, Namespace, BundleName),
    NodeRes = lists:zip(Nodes, Res),
    Errors = lists:filtermap(
        fun
            ({_Node, {ok, ok}}) ->
                false;
            ({_Node, {ok, {error, enoent}}}) ->
                false;
            ({Node, {ok, {error, Reason}}}) ->
                {true, #{node => Node, kind => file, reason => Reason}};
            ({Node, {Class, Reason}}) ->
                {true, #{node => Node, kind => rpc, reason => {Class, Reason}}}
        end,
        NodeRes
    ),
    case Errors of
        [] ->
            ok;
        [_ | _] ->
            {error, Errors}
    end.

-spec add_managed_file(maybe_namespace(), bundle_name(), file_kind(), iodata()) ->
    ok | {error, [#{node := node(), kind := file | rpc, reason := term()}]}.
add_managed_file(Namespace, BundleName, Kind, Contents) ->
    Nodes = emqx_bpapi:nodes_supporting_bpapi_version(?BPAPI, 1),
    Res = emqx_mgmt_api_certs_proto_v1:add_managed_file(
        Nodes, Namespace, BundleName, Kind, Contents
    ),
    NodeRes = lists:zip(Nodes, Res),
    Errors = lists:filtermap(
        fun
            ({_Node, {ok, ok}}) ->
                false;
            ({Node, {ok, {error, Reason}}}) ->
                {true, #{node => Node, kind => file, reason => Reason}};
            ({Node, {Class, Reason}}) ->
                {true, #{node => Node, kind => rpc, reason => {Class, Reason}}}
        end,
        NodeRes
    ),
    case Errors of
        [] ->
            ok;
        [_ | _] ->
            {error, Errors}
    end.

%%------------------------------------------------------------------------------
%% RPC Targets
%%------------------------------------------------------------------------------

-spec add_managed_file_v1(maybe_namespace(), bundle_name(), file_kind(), contents()) ->
    ok | {error, file:posix()}.
-doc #{since => <<"6.1.0">>}.
add_managed_file_v1(Namespace, BundleName, Kind, Contents) ->
    Filename = filename(Namespace, BundleName, Kind),
    maybe
        ok ?= filelib:ensure_dir(Filename),
        file:write_file(Filename, Contents)
    end.

-spec delete_managed_file_v1(maybe_namespace(), bundle_name(), file_kind()) ->
    ok | {error, file:posix()}.
-doc #{since => <<"6.1.0">>}.
delete_managed_file_v1(Namespace, BundleName, Kind) ->
    Filename = filename(Namespace, BundleName, Kind),
    file:delete(Filename).

-spec delete_bundle_v1(maybe_namespace(), bundle_name()) ->
    ok | {error, file:posix()}.
-doc #{since => <<"6.1.0">>}.
delete_bundle_v1(Namespace, BundleName) ->
    Dir = dir(Namespace, BundleName),
    file:del_dir_r(Dir).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

filename(Namespace, BundleName, ?FILE_KIND_KEY) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_KEY);
filename(Namespace, BundleName, ?FILE_KIND_CHAIN) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_CHAIN);
filename(Namespace, BundleName, ?FILE_KIND_CA) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_CA);
filename(Namespace, BundleName, ?FILE_KIND_ACC_KEY) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_ACC_KEY);
filename(Namespace, BundleName, ?FILE_KIND_KEY_PASSWORD) ->
    filename:join(dir(Namespace, BundleName), ?FILENAME_KEY_PASSWORD).

base_dir(?global_ns) ->
    DataDir = emqx:data_dir(),
    filename:join([DataDir, certs2, global]);
base_dir(Namespace0) when is_binary(Namespace0) ->
    DataDir = emqx:data_dir(),
    Namespace = escape_name(Namespace0),
    filename:join([DataDir, certs2, ns, Namespace]).

dir(Namespace, BundleName0) ->
    BaseDir = base_dir(Namespace),
    BundleName = escape_name(BundleName0),
    filename:join([BaseDir, BundleName]).

filename_to_kind(?FILENAME_KEY) ->
    {ok, ?FILE_KIND_KEY};
filename_to_kind(?FILENAME_CHAIN) ->
    {ok, ?FILE_KIND_CHAIN};
filename_to_kind(?FILENAME_CA) ->
    {ok, ?FILE_KIND_CA};
filename_to_kind(?FILENAME_ACC_KEY) ->
    {ok, ?FILE_KIND_ACC_KEY};
filename_to_kind(?FILENAME_KEY_PASSWORD) ->
    {ok, ?FILE_KIND_KEY_PASSWORD};
filename_to_kind(_) ->
    error.

escape_name(Name) ->
    uri_string:quote(Name).
