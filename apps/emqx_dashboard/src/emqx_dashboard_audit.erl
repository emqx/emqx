%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%% API
-export([log/2]).

%% todo filter high frequency events
-define(HIGH_FREQUENCY_EVENTS, [
    mqtt_subscribe,
    mqtt_unsubscribe,
    mqtt_subscribe_batch,
    mqtt_unsubscribe_batch,
    mqtt_publish,
    mqtt_publish_batch,
    kickout_client
]).

log(#{code := Code, method := Method} = Meta, Req) ->
    %% Keep level/2 and log_meta/1 inside of this ?AUDIT macro
    ?AUDIT(level(Method, Code), log_meta(Meta, Req)).

log_meta(Meta, Req) ->
    Meta1 = #{
        time => logger:timestamp(),
        from => from(Meta),
        source => source(Meta),
        duration_ms => duration_ms(Meta),
        source_ip => source_ip(Req),
        operation_type => operation_type(Meta),
        %% method for http filter api.
        http_method => maps:get(method, Meta),
        http_request => http_request(Meta),
        http_status_code => maps:get(code, Meta),
        operation_result => operation_result(Meta),
        node => node()
    },
    Meta2 = maps:without([req_start, req_end, method, headers, body, bindings, code], Meta),
    emqx_utils:redact(maps:merge(Meta2, Meta1)).

duration_ms(#{req_start := ReqStart, req_end := ReqEnd}) ->
    erlang:convert_time_unit(ReqEnd - ReqStart, native, millisecond).

from(Meta) ->
    case maps:find(auth_type, Meta) of
        {ok, jwt_token} ->
            dashboard;
        {ok, api_key} ->
            rest_api;
        error ->
            case maps:find(operation_id, Meta) of
                %% login api create jwt_token, so we don have authorization in it's headers
                {ok, <<"/login">>} -> dashboard;
                _ -> unknown
            end
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
        {ok, OperationId} -> lists:nth(2, binary:split(OperationId, <<"/">>));
        _ -> <<"unknown">>
    end.

http_request(Meta) ->
    maps:with([method, headers, bindings, body], Meta).

operation_result(#{failure := _}) -> failure;
operation_result(_) -> success.

level(get, _Code) -> debug;
level(_, Code) when Code >= 200 andalso Code < 300 -> info;
level(_, Code) when Code >= 300 andalso Code < 400 -> warning;
level(_, Code) when Code >= 400 andalso Code < 500 -> error;
level(_, _) -> critical.
