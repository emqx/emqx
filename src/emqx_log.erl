%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_log).

-compile({no_auto_import,[error/1]}).

-export([debug/1, debug/2,
         info/1, info/2,
         warning/1, warning/2,
         error/1, error/2,
         critical/1, critical/2]).

debug(Msg) ->
    lager:debug(Msg).
debug(Format, Args) ->
    lager:debug(Format, Args).

info(Msg) ->
    lager:info(Msg).
info(Format, Args) ->
    lager:info(Format, Args).

warning(Msg) ->
    lager:warning(Msg).
warning(Format, Args) ->
    lager:warning(Format, Args).

error(Msg) ->
    lager:error(Msg).
error(Format, Args) ->
    lager:error(Format, Args).

critical(Msg) ->
    lager:critical(Msg).
critical(Format, Args) ->
    lager:critical(Format, Args).

