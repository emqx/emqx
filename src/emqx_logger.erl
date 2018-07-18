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

debug(Msg) ->
    lager:debug(Msg).
debug(Format, Args) ->
    lager:debug(Format, Args).
debug(Metadata, Format, Args) when is_list(Metadata) ->
    lager:debug(Metadata, Format, Args).

info(Msg) ->
    lager:info(Msg).
info(Format, Args) ->
    lager:info(Format, Args).
info(Metadata, Format, Args) when is_list(Metadata) ->
    lager:info(Metadata, Format, Args).

warning(Msg) ->
    lager:warning(Msg).
warning(Format, Args) ->
    lager:warning(Format, Args).
warning(Metadata, Format, Args) when is_list(Metadata) ->
    lager:warning(Metadata, Format, Args).

error(Msg) ->
    lager:error(Msg).
error(Format, Args) ->
    lager:error(Format, Args).
error(Metadata, Format, Args) when is_list(Metadata) ->
    lager:error(Metadata, Format, Args).

critical(Msg) ->
    lager:critical(Msg).
critical(Format, Args) ->
    lager:critical(Format, Args).
critical(Metadata, Format, Args) when is_list(Metadata) ->
    lager:critical(Metadata, Format, Args).

