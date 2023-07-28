%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_impl_producer).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-type config() :: #{
    attributes_template := [#{key := binary(), value := binary()}],
    connect_timeout := emqx_schema:duration_ms(),
    max_retries := non_neg_integer(),
    ordering_key_template := binary(),
    payload_template := binary(),
    pubsub_topic := binary(),
    resource_opts := #{request_ttl := infinity | emqx_schema:duration_ms(), any() => term()},
    service_account_json := emqx_bridge_gcp_pubsub_client:service_account_json(),
    any() => term()
}.
-type state() :: #{
    attributes_template := #{emqx_placeholder:tmpl_token() => emqx_placeholder:tmpl_token()},
    client := emqx_bridge_gcp_pubsub_client:state(),
    ordering_key_template := emqx_placeholder:tmpl_token(),
    payload_template := emqx_placeholder:tmpl_token(),
    project_id := emqx_bridge_gcp_pubsub_client:project_id(),
    pubsub_topic := binary()
}.
-type headers() :: emqx_bridge_gcp_pubsub_client:headers().
-type body() :: emqx_bridge_gcp_pubsub_client:body().
-type status_code() :: emqx_bridge_gcp_pubsub_client:status_code().

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
on_start(InstanceId, Config0) ->
    ?SLOG(info, #{
        msg => "starting_gcp_pubsub_bridge",
        config => Config0
    }),
    Config = maps:update_with(service_account_json, fun emqx_utils_maps:binary_key_map/1, Config0),
    #{
        attributes_template := AttributesTemplate,
        ordering_key_template := OrderingKeyTemplate,
        payload_template := PayloadTemplate,
        pubsub_topic := PubSubTopic,
        service_account_json := #{<<"project_id">> := ProjectId}
    } = Config,
    case emqx_bridge_gcp_pubsub_client:start(InstanceId, Config) of
        {ok, Client} ->
            State = #{
                client => Client,
                attributes_template => preproc_attributes(AttributesTemplate),
                ordering_key_template => emqx_placeholder:preproc_tmpl(OrderingKeyTemplate),
                payload_template => emqx_placeholder:preproc_tmpl(PayloadTemplate),
                project_id => ProjectId,
                pubsub_topic => PubSubTopic
            },
            {ok, State};
        Error ->
            Error
    end.

-spec on_stop(resource_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, _State) ->
    emqx_bridge_gcp_pubsub_client:stop(InstanceId).

-spec on_get_status(resource_id(), state()) -> connected | disconnected.
on_get_status(_InstanceId, #{client := Client} = _State) ->
    emqx_bridge_gcp_pubsub_client:get_status(Client).

-spec on_query(
    resource_id(),
    {send_message, map()},
    state()
) ->
    {ok, map()}
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
) -> {ok, pid()} | {error, no_pool_worker_available}.
on_query_async(ResourceId, {send_message, Selected}, ReplyFunAndArgs, State) ->
    Requests = [{send_message, Selected}],
    ?TRACE(
        "QUERY_ASYNC",
        "gcp_pubsub_received",
        #{requests => Requests, connector => ResourceId, state => State}
    ),
    do_send_requests_async(State, Requests, ReplyFunAndArgs).

-spec on_batch_query(
    resource_id(),
    [{send_message, map()}],
    state()
) ->
    {ok, map()}
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
) -> {ok, pid()} | {error, no_pool_worker_available}.
on_batch_query_async(ResourceId, Requests, ReplyFunAndArgs, State) ->
    ?TRACE(
        "QUERY_ASYNC",
        "gcp_pubsub_received",
        #{requests => Requests, connector => ResourceId, state => State}
    ),
    do_send_requests_async(State, Requests, ReplyFunAndArgs).

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
    #{client := Client} = State,
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
    Result = emqx_bridge_gcp_pubsub_client:query_sync(Request, Client),
    QueryMode = sync,
    handle_result(Result, Request, QueryMode, InstanceId).

-spec do_send_requests_async(
    state(),
    [{send_message, map()}],
    {ReplyFun :: function(), Args :: list()}
) -> {ok, pid()} | {error, no_pool_worker_available}.
do_send_requests_async(State, Requests, ReplyFunAndArgs0) ->
    #{client := Client} = State,
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
    emqx_bridge_gcp_pubsub_client:query_async(
        Request, ReplyFunAndArgs, Client
    ).

-spec encode_payload(state(), Selected :: map()) ->
    #{
        data := binary(),
        attributes => #{binary() => binary()},
        'orderingKey' => binary()
    }.
encode_payload(State, Selected) ->
    #{
        attributes_template := AttributesTemplate,
        ordering_key_template := OrderingKeyTemplate,
        payload_template := PayloadTemplate
    } = State,
    Data = render_payload(PayloadTemplate, Selected),
    OrderingKey = render(OrderingKeyTemplate, Selected),
    Attributes = proc_attributes(AttributesTemplate, Selected),
    Payload0 = #{data => base64:encode(Data)},
    Payload1 = put_if(Payload0, attributes, Attributes, map_size(Attributes) > 0),
    put_if(Payload1, 'orderingKey', OrderingKey, OrderingKey =/= <<>>).

put_if(Acc, K, V, true) ->
    Acc#{K => V};
put_if(Acc, _K, _V, false) ->
    Acc.

-spec render_payload(emqx_placeholder:tmpl_token(), map()) -> binary().
render_payload([] = _Template, Selected) ->
    emqx_utils_json:encode(Selected);
render_payload(Template, Selected) ->
    render(Template, Selected).

render(Template, Selected) ->
    Opts = #{
        return => full_binary,
        var_trans => fun
            (undefined) -> <<>>;
            (X) -> emqx_utils_conv:bin(X)
        end
    },
    emqx_placeholder:proc_tmpl(Template, Selected, Opts).

-spec preproc_attributes([#{key := binary(), value := binary()}]) ->
    #{emqx_placeholder:tmpl_token() => emqx_placeholder:tmpl_token()}.
preproc_attributes(AttributesTemplate) ->
    lists:foldl(
        fun(#{key := K, value := V}, Acc) ->
            KT = emqx_placeholder:preproc_tmpl(K),
            VT = emqx_placeholder:preproc_tmpl(V),
            Acc#{KT => VT}
        end,
        #{},
        AttributesTemplate
    ).

-spec proc_attributes(#{emqx_placeholder:tmpl_token() => emqx_placeholder:tmpl_token()}, map()) ->
    #{binary() => binary()}.
proc_attributes(AttributesTemplate, Selected) ->
    maps:fold(
        fun(KT, VT, Acc) ->
            K = render(KT, Selected),
            case K =:= <<>> of
                true ->
                    Acc;
                false ->
                    V = render(VT, Selected),
                    Acc#{K => V}
            end
        end,
        #{},
        AttributesTemplate
    ).

-spec to_pubsub_request([#{data := binary()}]) -> binary().
to_pubsub_request(Payloads) ->
    emqx_utils_json:encode(#{messages => Payloads}).

-spec publish_path(state()) -> binary().
publish_path(
    _State = #{
        project_id := ProjectId,
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
        request => emqx_bridge_http_connector:redact_request(Request),
        connector => ResourceId,
        status_code => StatusCode,
        resp_body => RespBody
    }),
    Result;
handle_result({error, #{status_code := StatusCode}} = Result, Request, _QueryMode, ResourceId) ->
    ?SLOG(error, #{
        msg => "gcp_pubsub_error_response",
        request => emqx_bridge_http_connector:redact_request(Request),
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
            Result = {error, {recoverable_error, Reason}},
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
        _ ->
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Response)
    end.
