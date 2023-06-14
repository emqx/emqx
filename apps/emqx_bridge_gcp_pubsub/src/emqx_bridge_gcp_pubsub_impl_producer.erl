%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_impl_producer).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-type config() :: #{
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    pubsub_topic := binary(),
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    service_account_json := emqx_bridge_gcp_pubsub_connector:service_account_json(),
    any() => term()
}.
-type state() :: #{
    connector_state := emqx_bridge_gcp_pubsub_connector:state(),
    payload_template := emqx_placeholder:tmpl_token(),
    pubsub_topic := binary()
}.
-type headers() :: emqx_bridge_gcp_pubsub_connector:headers().
-type body() :: emqx_bridge_gcp_pubsub_connector:body().
-type status_code() :: emqx_bridge_gcp_pubsub_connector:status_code().

%% `emqx_resource' API
-export([
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_batch_query/3,
    on_batch_query_async/4,
    on_get_status/2
]).

-export([reply_delegator/2]).

%%-------------------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------------------

callback_mode() -> async_if_possible.

query_mode(_Config) -> async.

-spec on_start(resource_id(), config()) -> {ok, state()} | {error, term()}.
on_start(InstanceId, Config) ->
    #{
        payload_template := PayloadTemplate,
        pubsub_topic := PubSubTopic
    } = Config,
    case emqx_bridge_gcp_pubsub_connector:on_start(InstanceId, Config) of
        {ok, ConnectorState} ->
            State = #{
                connector_state => ConnectorState,
                payload_template => emqx_placeholder:preproc_tmpl(PayloadTemplate),
                pubsub_topic => PubSubTopic
            },
            {ok, State};
        Error ->
            Error
    end.

-spec on_stop(resource_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, #{connector_state := ConnectorState}) ->
    emqx_bridge_gcp_pubsub_connector:on_stop(InstanceId, ConnectorState);
on_stop(InstanceId, undefined = _State) ->
    emqx_bridge_gcp_pubsub_connector:on_stop(InstanceId, undefined).

-spec on_get_status(resource_id(), state()) -> connected | disconnected.
on_get_status(InstanceId, #{connector_state := ConnectorState} = _State) ->
    emqx_bridge_gcp_pubsub_connector:on_get_status(InstanceId, ConnectorState).

-spec on_query(
    resource_id(),
    {send_message, map()},
    state()
) ->
    {ok, status_code(), headers()}
    | {ok, status_code(), headers(), body()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
on_query(ResourceId, {send_message, Selected}, State) ->
    Requests = [{send_message, Selected}],
    ?TRACE(
        "QUERY_SYNC",
        "gcp_pubsub_received",
        #{requests => Requests, connector => ResourceId, state => State}
    ),
    do_send_requests_sync(State, Requests, ResourceId).

-spec on_query_async(
    resource_id(),
    {send_message, map()},
    {ReplyFun :: function(), Args :: list()},
    state()
) -> {ok, pid()}.
on_query_async(ResourceId, {send_message, Selected}, ReplyFunAndArgs, State) ->
    Requests = [{send_message, Selected}],
    ?TRACE(
        "QUERY_ASYNC",
        "gcp_pubsub_received",
        #{requests => Requests, connector => ResourceId, state => State}
    ),
    do_send_requests_async(State, Requests, ReplyFunAndArgs, ResourceId).

-spec on_batch_query(
    resource_id(),
    [{send_message, map()}],
    state()
) ->
    {ok, status_code(), headers()}
    | {ok, status_code(), headers(), body()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
on_batch_query(ResourceId, Requests, State) ->
    ?TRACE(
        "QUERY_SYNC",
        "gcp_pubsub_received",
        #{requests => Requests, connector => ResourceId, state => State}
    ),
    do_send_requests_sync(State, Requests, ResourceId).

-spec on_batch_query_async(
    resource_id(),
    [{send_message, map()}],
    {ReplyFun :: function(), Args :: list()},
    state()
) -> {ok, pid()}.
on_batch_query_async(ResourceId, Requests, ReplyFunAndArgs, State) ->
    ?TRACE(
        "QUERY_ASYNC",
        "gcp_pubsub_received",
        #{requests => Requests, connector => ResourceId, state => State}
    ),
    do_send_requests_async(State, Requests, ReplyFunAndArgs, ResourceId).

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

-spec do_send_requests_sync(
    state(),
    [{send_message, map()}],
    resource_id()
) ->
    {ok, status_code(), headers()}
    | {ok, status_code(), headers(), body()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
do_send_requests_sync(State, Requests, InstanceId) ->
    #{connector_state := ConnectorState} = State,
    Payloads =
        lists:map(
            fun({send_message, Selected}) ->
                encode_payload(State, Selected)
            end,
            Requests
        ),
    Body = to_pubsub_request(Payloads),
    Path = publish_path(State),
    Method = post,
    Request = {prepared_request, {Method, Path, Body}},
    Result = emqx_bridge_gcp_pubsub_connector:on_query(InstanceId, Request, ConnectorState),
    QueryMode = sync,
    handle_result(Result, Request, QueryMode, InstanceId).

-spec do_send_requests_async(
    state(),
    [{send_message, map()}],
    {ReplyFun :: function(), Args :: list()},
    resource_id()
) -> {ok, pid()}.
do_send_requests_async(State, Requests, ReplyFunAndArgs0, InstanceId) ->
    #{connector_state := ConnectorState} = State,
    Payloads =
        lists:map(
            fun({send_message, Selected}) ->
                encode_payload(State, Selected)
            end,
            Requests
        ),
    Body = to_pubsub_request(Payloads),
    Path = publish_path(State),
    Method = post,
    Request = {prepared_request, {Method, Path, Body}},
    ReplyFunAndArgs = {fun ?MODULE:reply_delegator/2, [ReplyFunAndArgs0]},
    emqx_bridge_gcp_pubsub_connector:on_query_async(
        InstanceId, Request, ReplyFunAndArgs, ConnectorState
    ).

-spec encode_payload(state(), Selected :: map()) -> #{data := binary()}.
encode_payload(_State = #{payload_template := PayloadTemplate}, Selected) ->
    Interpolated =
        case PayloadTemplate of
            [] -> emqx_utils_json:encode(Selected);
            _ -> emqx_placeholder:proc_tmpl(PayloadTemplate, Selected)
        end,
    #{data => base64:encode(Interpolated)}.

-spec to_pubsub_request([#{data := binary()}]) -> binary().
to_pubsub_request(Payloads) ->
    emqx_utils_json:encode(#{messages => Payloads}).

-spec publish_path(state()) -> binary().
publish_path(
    _State = #{
        connector_state := #{project_id := ProjectId},
        pubsub_topic := PubSubTopic
    }
) ->
    <<"/v1/projects/", ProjectId/binary, "/topics/", PubSubTopic/binary, ":publish">>.

handle_result({error, Reason}, _Request, QueryMode, ResourceId) when
    Reason =:= econnrefused;
    %% this comes directly from `gun'...
    Reason =:= {closed, "The connection was lost."};
    Reason =:= timeout
->
    ?tp(
        warning,
        gcp_pubsub_request_failed,
        #{
            reason => Reason,
            query_mode => QueryMode,
            recoverable_error => true,
            connector => ResourceId
        }
    ),
    {error, {recoverable_error, Reason}};
handle_result(
    {error, #{status_code := StatusCode, body := RespBody}} = Result,
    Request,
    _QueryMode,
    ResourceId
) ->
    ?SLOG(error, #{
        msg => "gcp_pubsub_error_response",
        request => emqx_connector_http:redact_request(Request),
        connector => ResourceId,
        status_code => StatusCode,
        resp_body => RespBody
    }),
    Result;
handle_result({error, #{status_code := StatusCode}} = Result, Request, _QueryMode, ResourceId) ->
    ?SLOG(error, #{
        msg => "gcp_pubsub_error_response",
        request => emqx_connector_http:redact_request(Request),
        connector => ResourceId,
        status_code => StatusCode
    }),
    Result;
handle_result({error, Reason} = Result, _Request, QueryMode, ResourceId) ->
    ?tp(
        error,
        gcp_pubsub_request_failed,
        #{
            reason => Reason,
            query_mode => QueryMode,
            recoverable_error => false,
            connector => ResourceId
        }
    ),
    Result;
handle_result({ok, _} = Result, _Request, _QueryMode, _ResourceId) ->
    Result.

reply_delegator(ReplyFunAndArgs, Response) ->
    case Response of
        {error, Reason} when
            Reason =:= econnrefused;
            %% this comes directly from `gun'...
            Reason =:= {closed, "The connection was lost."};
            Reason =:= timeout
        ->
            Result1 = {error, {recoverable_error, Reason}},
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result1);
        _ ->
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Response)
    end.
