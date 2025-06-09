%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_source).

-type source_type() :: atom().
-type source_state() :: #{
    type := source_type(),
    enable := boolean(),
    resource_id => emqx_resource:resource_id(),
    _ => _
}.
-type source() :: emqx_config:config().
-type raw_source() :: map().
-type match_result() :: {matched, allow | deny | ignore} | nomatch | ignore.

-export_type([
    source_type/0,
    source/0,
    source_state/0,
    raw_source/0,
    match_result/0
]).

%% Initialize authz backend.
-callback create(source()) -> source_state().

%% Update authz backend.
%% Change configuration, or simply enable/disable
-callback update(source_state(), source()) -> source_state().

%% Destroy authz backend.
%% Make cleanup of all allocated data.
%% An authz backend will not be used after `destroy`.
-callback destroy(source_state()) -> ok.

%% Authorize client action.
-callback authorize(
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    source_state()
) -> match_result().

%% Convert filepath values to the content of the files.
-callback write_files(raw_source()) -> raw_source() | no_return().

%% Convert filepath values to the content of the files.
-callback read_files(raw_source()) -> raw_source() | no_return().

%% Merge default values to the source, for example, for exposing via API
-callback format_for_api(raw_source()) -> raw_source().

-optional_callbacks([
    write_files/1,
    read_files/1,
    format_for_api/1
]).
