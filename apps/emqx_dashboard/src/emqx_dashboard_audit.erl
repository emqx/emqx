%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_audit).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/http_api.hrl").
%% API
-export([log/2, log_fun/0, importance/1]).

%% In the previous versions,
%% this module used the request method to determine whether the request should be logged,
%% but here are some exceptions:
%% 1. the OIDC callback uses the `GET` method, but it is important
%% 2. some endpoints (called frequency requests) use the `POST` method,
%%    but most of the time we do not want to log them
%% So an auxiliary `importance` metadata was introduced.
%%
%% The strategy is:
%% 1. Use `high` to mark an important `GET` method
%% 2. Use `low` to mark the frequency methods
%% 3. `medium` is the default importance and is set automatically

-define(AUDIT_IMPORTANCE_HIGH, 100).
-define(AUDIT_IMPORTANCE_MEDIUM, 60).
-define(AUDIT_IMPORTANCE_LOW, 30).

-define(CODE_METHOD_NOT_ALLOWED, 405).

log_fun() ->
    {emqx_dashboard_audit, log, #{importance => medium}}.

importance(Level) when
    Level =:= high;
    Level =:= medium;
    Level =:= low
->
    #{importance => Level}.

log(#{code := Code, method := Method, importance := Importance} = Meta, Req) ->
    %% Keep level/2 and log_meta/1 inside of this ?AUDIT macro
    ImportanceNum = importance_to_num(Code, Importance),
    ?AUDIT(level(ImportanceNum, Method, Code), log_meta(ImportanceNum, Meta, Req)).

log_meta(Importance, #{method := get} = _Meta, _Req) when Importance =< ?AUDIT_IMPORTANCE_MEDIUM ->
    undefined;
log_meta(Importance, Meta, Req) ->
    #{method := Method} = Meta,
    case (Importance =< ?AUDIT_IMPORTANCE_LOW) andalso ignore_high_frequency_request() of
        true ->
            undefined;
        false ->
            Code = maps:get(code, Meta),
            Meta1 = #{
                time => logger:timestamp(),
                from => from(Meta),
                source => source(Meta),
                duration_ms => duration_ms(Meta),
                source_ip => source_ip(Req),
                operation_type => operation_type(Meta),
                %% method for http filter api.
                http_method => Method,
                http_request => http_request(Meta),
                http_status_code => Code,
                operation_result => operation_result(Code, Meta),
                node => node()
            },
            Meta2 = maps:without([req_start, req_end, method, headers, body, bindings, code], Meta),
            emqx_utils:redact(maps:merge(Meta2, Meta1))
    end.

duration_ms(#{req_start := ReqStart, req_end := ReqEnd}) ->
    erlang:convert_time_unit(ReqEnd - ReqStart, native, millisecond).

from(#{auth_type := jwt_token}) ->
    dashboard;
from(#{auth_type := api_key}) ->
    rest_api;
from(#{log_from := From}) ->
    From;
from(#{code := Code} = Meta) when Code =:= 401 orelse Code =:= 403 ->
    case maps:find(failure, Meta) of
        {ok, #{code := 'BAD_API_KEY_OR_SECRET'}} -> rest_api;
        {ok, #{code := 'UNAUTHORIZED_ROLE', message := ?API_KEY_NOT_ALLOW_MSG}} -> rest_api;
        %% 'TOKEN_TIME_OUT' 'BAD_TOKEN' is dashboard code.
        _ -> dashboard
    end;
from(_) ->
    unknown.

source(#{source := Source}) -> Source;
source(#{log_source := Source}) -> Source;
source(_Meta) -> <<"">>.

source_ip(Req) ->
    case cowboy_req:header(<<"x-forwarded-for">>, Req, undefined) of
        undefined ->
            {RemoteIP, _} = cowboy_req:peer(Req),
            iolist_to_binary(inet:ntoa(RemoteIP));
        Addresses ->
            hd(binary:split(Addresses, <<",">>))
    end.

operation_type(Meta) ->
    case maps:find(operation_id, Meta) of
        {ok, OperationId} ->
            lists:nth(2, binary:split(OperationId, <<"/">>, [global]));
        _ ->
            <<"unknown">>
    end.

http_request(Meta) ->
    maps:with([method, headers, bindings, body], Meta).

operation_result(302, _) -> success;
operation_result(Code, _) when Code >= 300 -> failure;
operation_result(_, #{failure := _}) -> failure;
operation_result(_, _) -> success.

%%
level(?AUDIT_IMPORTANCE_HIGH, _, _) -> warning;
level(_, get, _Code) -> debug;
level(_, _, Code) when Code >= 200 andalso Code < 300 -> info;
level(_, _, Code) when Code >= 300 andalso Code < 400 -> warning;
level(_, _, Code) when Code >= 400 andalso Code < 500 -> error;
level(_, _, _) -> critical.

ignore_high_frequency_request() ->
    emqx_conf:get([log, audit, ignore_high_frequency_request], true).

%% This is a special case.
%% An illegal request (e.g. A `GET` request to a `POST`-only endpoint) does not have metadata,
%% its `importance` is the default value,
%% so we have to manually increase the `importance` to record this request.
importance_to_num(?CODE_METHOD_NOT_ALLOWED, _) ->
    ?AUDIT_IMPORTANCE_HIGH;
importance_to_num(_, high) ->
    ?AUDIT_IMPORTANCE_HIGH;
importance_to_num(_, medium) ->
    ?AUDIT_IMPORTANCE_MEDIUM;
importance_to_num(_, low) ->
    ?AUDIT_IMPORTANCE_LOW.
