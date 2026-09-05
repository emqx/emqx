%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_audit).

-include_lib("emqx/include/logger.hrl").
%% API
-export([log/2, log_fun/0, importance/1]).
%% Exported for direct unit testing of the redaction/shape logic
%% below, without going through the ?AUDIT macro (which writes to
%% mnesia and needs the whole app booted).
-export([log_meta/3]).

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
                from => from(Meta, Req),
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
            Meta2 = maps:without(
                [
                    req_start,
                    req_end,
                    method,
                    headers,
                    body,
                    bindings,
                    code,
                    query_string,
                    namespace
                ],
                Meta
            ),
            emqx_utils:redact(maps:merge(Meta2, Meta1))
    end.

duration_ms(#{req_start := ReqStart, req_end := ReqEnd}) ->
    erlang:convert_time_unit(ReqEnd - ReqStart, native, millisecond).

from(#{auth_type := jwt_token}, _Req) ->
    dashboard;
from(#{auth_type := api_key}, _Req) ->
    rest_api;
from(#{log_from := From}, _Req) ->
    From;
from(#{code := Code} = Meta, Req) when Code =:= 401 orelse Code =:= 403 ->
    %% Auth failed before `auth_type` could be populated by the authoriser.
    %% Distinguish by the request's `Authorization` header: Basic ⇒ API key,
    %% Bearer (or anything else) ⇒ dashboard.  This avoids matching against
    %% the response body message text, which now varies by RBAC reason.
    case maps:find(failure, Meta) of
        {ok, #{code := 'BAD_API_KEY_OR_SECRET'}} ->
            rest_api;
        _ ->
            case cowboy_req:parse_header(<<"authorization">>, Req) of
                {basic, _, _} -> rest_api;
                _ -> dashboard
            end
    end;
from(_, _Req) ->
    unknown.

%% `Source' may be `emqx_dashboard_admin:dashboard_username()' for an SSO
%% session, i.e. `{Backend, Name}' instead of a plain binary. Format it here,
%% at the point the audit event is built for both file logging and DB
%% storage, so neither carries an unformatted tuple.
source(#{source := Source}) -> emqx_dashboard_admin:format_username(Source);
source(#{log_source := Source}) -> emqx_dashboard_admin:format_username(Source);
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
    %% `namespace' is the resolved target namespace, set by handlers that call
    %% `minirest_handler:update_log_meta/1' (see emqx_topic_metrics2_api for
    %% an example). It is not always the same as the `ns' query param: a
    %% namespaced admin's namespace comes from their dashboard token, not
    %% from the request, so recording it here also covers requests with no
    %% `ns' query param at all. `maps:with/2' already omits it when absent
    %% from `Meta', same as it does for `method'/`headers'/`bindings'/`body'.
    Request0 =
        case maps:with([method, headers, bindings, body, namespace], Meta) of
            #{body := Body} = Request when is_binary(Body) ->
                Request#{body => <<"******">>};
            #{body := _} = Request ->
                case is_sensitive_body_operation(Meta) of
                    true -> Request#{body => <<"******">>};
                    false -> Request
                end;
            Request ->
                Request
        end,
    %% `query_string' is the parsed request query params (a map, not a raw
    %% binary), so the `emqx_utils:redact/1' call at the end of `log_meta/3'
    %% can see the keys and redact sensitive ones by name, same as it does
    %% for `headers' and `body' above. Handled separately from the `with/2'
    %% above because an empty query string must be omitted, not recorded as
    %% `query_string => #{}'.
    maybe_put(query_string, non_empty_map(maps:get(query_string, Meta, undefined)), Request0).

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

non_empty_map(Map) when is_map(Map), map_size(Map) > 0 -> Map;
non_empty_map(_) -> undefined.

%% Endpoints whose request body carries a secret under a key name that
%% the generic key-name based redaction does not cover.
is_sensitive_body_operation(#{operation_id := <<"/license">>}) -> true;
is_sensitive_body_operation(_) -> false.

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
