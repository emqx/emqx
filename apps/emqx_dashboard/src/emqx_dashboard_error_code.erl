%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_error_code).

-include_lib("emqx_utils/include/emqx_http_api.hrl").

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
