%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_source).

-type source_type() :: atom().
-type source() :: #{type => source_type(), _ => _}.
-type raw_source() :: map().
-type match_result() :: {matched, allow | deny | ignore} | nomatch | ignore.

-export_type([
    source_type/0,
    source/0,
    match_result/0
]).

%% Initialize authz backend.
%% Populate the passed configuration map with necessary data,
%% like `ResourceID`s
-callback create(source()) -> source().

%% Update authz backend.
%% Change configuration, or simply enable/disable
-callback update(source()) -> source().

%% Destroy authz backend.
%% Make cleanup of all allocated data.
%% An authz backend will not be used after `destroy`.
-callback destroy(source()) -> ok.

%% Authorize client action.
-callback authorize(
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    source()
) -> match_result().

%% Convert filepath values to the content of the files.
-callback write_files(raw_source()) -> raw_source() | no_return().

%% Convert filepath values to the content of the files.
-callback read_files(raw_source()) -> raw_source() | no_return().

%% Merge default values to the source, for example, for exposing via API
-callback format_for_api(raw_source()) -> raw_source().

-optional_callbacks([
    update/1,
    write_files/1,
    read_files/1,
    format_for_api/1
]).
