%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Get authz text description.
-callback description() -> string().

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
