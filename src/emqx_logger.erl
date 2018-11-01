%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger).

-compile({no_auto_import,[error/1]}).

-export([debug/1, debug/2, debug/3]).
-export([info/1, info/2, info/3]).
-export([warning/1, warning/2, warning/3]).
-export([error/1, error/2, error/3]).
-export([critical/1, critical/2, critical/3]).

-export([add_proc_metadata/1]).

debug(Msg) ->
    logger:debug(Msg).
debug(Format, Args) ->
    logger:debug(Format, Args).
debug(Metadata, Format, Args) when is_map(Metadata) ->
    logger:debug(Format, Args, Metadata).

info(Msg) ->
    logger:info(Msg).
info(Format, Args) ->
    logger:info(Format, Args).
info(Metadata, Format, Args) when is_map(Metadata) ->
    logger:info(Format, Args, Metadata).

warning(Msg) ->
    logger:warning(Msg).
warning(Format, Args) ->
    logger:warning(Format, Args).
warning(Metadata, Format, Args) when is_map(Metadata) ->
    logger:warning(Format, Args, Metadata).

error(Msg) ->
    logger:error(Msg).
error(Format, Args) ->
    logger:error(Format, Args).
error(Metadata, Format, Args) when is_map(Metadata) ->
    logger:error(Format, Args, Metadata).

critical(Msg) ->
    logger:critical(Msg).
critical(Format, Args) ->
    logger:critical(Format, Args).
critical(Metadata, Format, Args) when is_map(Metadata) ->
    logger:critical(Format, Args, Metadata).

add_proc_metadata(Meta) ->
    case logger:get_process_metadata() of
        undefined ->
            logger:set_process_metadata(Meta);
        OldMeta ->
            logger:set_process_metadata(maps:merge(OldMeta, Meta))
    end.