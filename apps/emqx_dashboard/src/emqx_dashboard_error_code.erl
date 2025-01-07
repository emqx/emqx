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

-module(emqx_dashboard_error_code).

-include_lib("emqx/include/http_api.hrl").

-export([
    all/0,
    list/0,
    look_up/1,
    description/1,
    format/1
]).

all() ->
    [Name || {Name, _Description} <- ?ERROR_CODES].

list() ->
    [format(Code) || Code <- ?ERROR_CODES].

look_up(Code) ->
    look_up(Code, ?ERROR_CODES).
look_up(_Code, []) ->
    {error, not_found};
look_up(Code, [{Code, Description} | _List]) ->
    {ok, format({Code, Description})};
look_up(Code, [_ | List]) ->
    look_up(Code, List).

description(Code) ->
    description(Code, ?ERROR_CODES).
description(_Code, []) ->
    {error, not_found};
description(Code, [{Code, Description} | _List]) ->
    {ok, Description};
description(Code, [_ | List]) ->
    description(Code, List).

format({Code, Description}) ->
    #{
        code => Code,
        description => Description
    }.
