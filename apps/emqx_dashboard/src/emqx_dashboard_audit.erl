%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([log/2]).

%% filter high frequency events
-define(HIGH_FREQUENCY_REQUESTS, [
    <<"/publish">>,
    <<"/clients/:clientid/subscribe">>,
    <<"/clients/:clientid/unsubscribe">>,
    <<"/publish/bulk">>,
    <<"/clients/:clientid/unsubscribe/bulk">>,
    <<"/clients/:clientid/subscribe/bulk">>,
    <<"/clients/kickout/bulk">>
]).

log(#{code := Code, method := Method} = Meta, Req) ->
    %% Keep level/2 and log_meta/1 inside of this ?AUDIT macro
    ?AUDIT(level(Method, Code), log_meta(Meta, Req)).

log_meta(Meta, Req) ->
    #{operation_id := OperationId, method := Method} = Meta,
    case
        Method =:= get orelse
            (lists:member(OperationId, ?HIGH_FREQUENCY_REQUESTS) andalso
                ignore_high_frequency_request())
    of
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
from(#{operation_id := <<"/login">>}) ->
    dashboard;
from(#{code := Code} = Meta) when Code =:= 401 orelse Code =:= 403 ->
    case maps:find(failure, Meta) of
        {ok, #{code := 'BAD_API_KEY_OR_SECRET'}} -> rest_api;
        {ok, #{code := 'UNAUTHORIZED_ROLE', message := ?API_KEY_NOT_ALLOW_MSG}} -> rest_api;
        %% 'TOKEN_TIME_OUT' 'BAD_TOKEN' is dashboard code.
        _ -> dashboard
    end.

source(#{source := Source}) -> Source;
source(#{operation_id := <<"/login">>, body := #{<<"username">> := Username}}) -> Username;
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

operation_result(Code, _) when Code >= 300 -> failure;
operation_result(_, #{failure := _}) -> failure;
operation_result(_, _) -> success.

level(get, _Code) -> debug;
level(_, Code) when Code >= 200 andalso Code < 300 -> info;
level(_, Code) when Code >= 300 andalso Code < 400 -> warning;
level(_, Code) when Code >= 400 andalso Code < 500 -> error;
level(_, _) -> critical.

ignore_high_frequency_request() ->
    emqx_conf:get([log, audit, ignore_high_frequency_request], true).
