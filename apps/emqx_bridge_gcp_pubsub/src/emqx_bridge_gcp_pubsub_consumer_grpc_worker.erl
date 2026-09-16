%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_grpc_worker).

-behaviour(gen_statem).

%% API
-export([
    start_link/1
]).

%% `gen_statem' API
-export([
    callback_mode/0,
    init/1,
    terminate/3,
    handle_event/4
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include("emqx_bridge_gcp_pubsub_consumer_grpc.hrl").

-define(keep_state_and_data, keep_state_and_data).
-define(keep_state(DATA), {keep_state, DATA}).
-define(keep_state_actions(DATA, ACTIONS), {keep_state, DATA, ACTIONS}).
-define(repeat_state(DATA), {repeat_state, DATA}).
-define(repeat_state_actions(DATA, ACTIONS), {repeat_state, DATA, ACTIONS}).
-define(next_state(STATE, DATA), {next_state, STATE, DATA}).
-define(state_timeout(TIME, CONTENT), {state_timeout, TIME, CONTENT}).

-record(handle, {handle, stream}).

%% States
-define(s_update_subscription, s_update_subscription).
-define(s_create_subscription, s_create_subscription).
-define(s_pull, s_pull).

%% calls/casts/infos/timeouts
-record(retry_subscription, {}).
-record(retry_pull, {}).

-type state() :: ?s_update_subscription | ?s_create_subscription | ?s_pull.
-type data() :: #{
    ?ack_deadline := 10..600,
    ?auth_ctx := emqx_bridge_gcp_pubsub_client:auth_ctx(),
    ?client_pool := _,
    ?handle := ?undefined | handle(),
    ?hookpoints := [_],
    ?idx := pos_integer(),
    ?max_outstanding_messages := non_neg_integer(),
    ?namespace := emqx_config:maybe_namespace(),
    ?pool := _,
    ?request_ttl := timeout(),
    ?pending_acks := [ack_id()],
    ?source_res_id := binary(),
    ?subscription_resource := binary(),
    ?topic_resource := binary()
}.

-type handle() :: #handle{}.
-type ack_id() :: binary().

-define(SERVICE, 'google.pubsub.v1.Subscriber').
-define(PROTO_MODULE, 'emqx_gcp_protos_gen_pubsub_pb').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType), #{
    path => Path,
    service => ?SERVICE,
    message_type => MessageType,
    marshal => ?MARSHAL(Req),
    unmarshal => ?UNMARSHAL(Resp)
}).

-define(DEFAULT_SUB_REQ_TIMEOUT, 60_000).

-ifdef(TEST).
-define(RETRY_SUB_TIMEOUT, 1_000).
-define(RETRY_PULL_TIMEOUT, 1_000).
-else.
-define(RETRY_SUB_TIMEOUT, 10_000).
-define(RETRY_PULL_TIMEOUT, 10_000).
-endif.

-type event_handler_result() :: gen_statem:event_handler_result(state(), data()).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Opts) ->
    gen_statem:start_link(?MODULE, Opts, []).

%%------------------------------------------------------------------------------
%% `gen_statem' API
%%------------------------------------------------------------------------------

callback_mode() ->
    [handle_event_function, state_enter].

-spec init(_) -> gen_statem:init_result(?s_update_subscription, data()).
init(Opts) ->
    process_flag(trap_exit, true),
    #{
        ack_deadline := AckDeadline,
        auth_ctx := AuthCtx,
        client_pool := ClientPool,
        hookpoints := Hookpoints,
        idx := Idx,
        max_outstanding_messages := MaxOutstandingMsgs,
        namespace := Namespace,
        pool := Pool,
        request_ttl := RequestTTL,
        source_name := SourceName,
        source_res_id := SourceResId,
        topic := Topic
    } = Opts,
    #{project_id := ProjectId} = AuthCtx,
    proc_lib:set_label({gcp_pubsub_consumer_grpc_worker, SourceResId}),
    true = gproc_pool:connect_worker(Pool, {Pool, Idx}),
    SubscriptionId =
        emqx_bridge_gcp_pubsub_consumer_worker:subscription_id(
            SourceName,
            Topic,
            ProjectId
        ),
    SubscriptionResource =
        emqx_bridge_gcp_pubsub_consumer_worker:subscription_resource(
            ProjectId,
            SubscriptionId
        ),
    TopicResource =
        emqx_bridge_gcp_pubsub_consumer_worker:topic_resource(
            ProjectId,
            Topic
        ),
    Data = #{
        ?ack_deadline => AckDeadline,
        ?auth_ctx => AuthCtx,
        ?client_pool => ClientPool,
        ?handle => ?undefined,
        ?hookpoints => Hookpoints,
        ?idx => Idx,
        ?max_outstanding_messages => MaxOutstandingMsgs,
        ?namespace => Namespace,
        ?pending_acks => [],
        ?pool => Pool,
        ?request_ttl => RequestTTL,
        ?source_res_id => SourceResId,
        ?subscription_resource => SubscriptionResource,
        ?topic_resource => TopicResource
    },
    {ok, ?s_update_subscription, Data}.

terminate(_Reason, _State, Data) ->
    #{?pool := Pool, ?idx := Idx} = Data,
    gproc_pool:disconnect_worker(Pool, {Pool, Idx}),
    maybe
        #handle{stream = Stream} ?= maps:get(?handle, Data),
        grpc_client:close_async(Stream)
    end,
    ok.

%% `?s_update_subscription`
handle_event(enter, _OldState, ?s_update_subscription, Data) ->
    ?tp("gcp_pubsub_consumer_enter_state", #{s => ?s_update_subscription}),
    handle_enter_update_subscription(Data);
handle_event(
    state_timeout,
    #handle{handle = H},
    ?s_update_subscription,
    #{?handle := #handle{handle = H}} = Data0
) ->
    handle_update_subscription_timeout(Data0);
handle_event(
    state_timeout,
    #retry_subscription{},
    ?s_update_subscription,
    Data0
) ->
    do_update_subscription(Data0);
handle_event(
    info,
    {grpc_reply, H, Res},
    ?s_update_subscription,
    #{?handle := #handle{handle = H}} = Data0
) ->
    handle_update_subscription_response(Res, Data0);
handle_event(
    info,
    {'DOWN', H, _, _, Reason},
    ?s_update_subscription,
    #{?handle := #handle{handle = H}} = Data0
) ->
    Res = {error, {grpc_client_down, Reason}},
    handle_update_subscription_response(Res, Data0);
%% `?s_create_subscription`
handle_event(enter, _OldState, ?s_create_subscription, Data) ->
    ?tp("gcp_pubsub_consumer_enter_state", #{s => ?s_create_subscription}),
    handle_enter_create_subscription(Data);
handle_event(
    state_timeout,
    #handle{handle = H},
    ?s_create_subscription,
    #{?handle := #handle{handle = H}} = Data0
) ->
    handle_create_subscription_timeout(Data0);
handle_event(
    state_timeout,
    #retry_subscription{},
    ?s_create_subscription,
    Data0
) ->
    do_create_subscription(Data0);
handle_event(
    info,
    {grpc_reply, H, Res},
    ?s_create_subscription,
    #{?handle := #handle{handle = H}} = Data0
) ->
    handle_create_subscription_response(Res, Data0);
handle_event(
    info,
    {'DOWN', H, _, _, Reason},
    ?s_create_subscription,
    #{?handle := #handle{handle = H}} = Data0
) ->
    Res = {error, {grpc_client_down, Reason}},
    handle_create_subscription_response(Res, Data0);
%% `?s_pull`
handle_event(enter, _OldState, ?s_pull, Data) ->
    ?tp("gcp_pubsub_consumer_enter_state", #{s => ?s_pull}),
    handle_enter_pull(Data);
handle_event(
    info,
    {grpc_reply, H, Res},
    ?s_pull,
    #{?handle := #handle{handle = H}} = Data0
) ->
    handle_pull_response(Res, Data0);
handle_event(
    state_timeout,
    #retry_pull{},
    ?s_pull,
    Data0
) ->
    do_pull(Data0);
handle_event(
    info,
    {'DOWN', H, _, _, Reason},
    ?s_pull,
    #{?handle := #handle{handle = H}} = Data0
) ->
    Res = {error, {grpc_client_down, Reason}},
    handle_pull_response(Res, Data0);
%% common
handle_event(_EventType, _EventContent, _State, _Data) ->
    ?keep_state_and_data.

%%------------------------------------------------------------------------------
%% Top state event handlers
%%------------------------------------------------------------------------------

%%==========================
%% Update sub
%%==========================
-spec handle_enter_update_subscription(data()) -> gen_statem:state_enter_result(state(), data()).
handle_enter_update_subscription(Data0) ->
    Data1 = Data0#{?handle := ?undefined},
    do_update_subscription(Data1).

-spec do_update_subscription(data()) -> event_handler_result().
do_update_subscription(Data0) ->
    Opts = grpc_opts(Data0),
    Req = update_subscription_req(Data0),
    maybe
        {ok, Metadata} ?= grpc_meta(Data0),
        {ok, Stream} ?= do_update_subscription_impl(Metadata, Opts),
        ok ?= grpc_send(Stream, Req, fin, Opts),
        Handle0 = grpc_client:async_install_receiver(Stream, #{mode => once}),
        Handle = #handle{handle = Handle0, stream = Stream},
        Data = Data0#{?handle := Handle},
        Timeout = req_timeout(Data),
        ?keep_state_actions(Data, [?state_timeout(Timeout, Handle)])
    else
        {error, Reason} ->
            handle_update_subscription_error(Reason, Data0)
    end.

-spec handle_update_subscription_timeout(data()) -> event_handler_result().
handle_update_subscription_timeout(Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    ?tp(warning, "gcp_pubsub_consumer_update_subscription_timeout", #{
        source_res_id => SourceResId
    }),
    {#handle{stream = Stream}, Data} = maps_swap(?handle, ?undefined, Data0),
    grpc_client:close_async(Stream),
    ?repeat_state(Data).

-spec handle_update_subscription_response({ok, _} | {error, _}, data()) -> event_handler_result().
handle_update_subscription_response({ok, ResRaw}, Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    {#handle{stream = Stream}, Data} = maps_swap(?handle, ?undefined, Data0),
    %% no need to cancel the stream; it already closed (non-streaming).
    case map_grpc_reply(ResRaw, Stream) of
        {done, [#{name := _}], {ok, _}} ->
            ?next_state(?s_pull, Data);
        {done, _, {not_found, _}} ->
            %% we optimistically try to first update the subscription, assuming we're
            %% restarting the source most times.  if the subscription doesn't exist, it's
            %% either the first run of the source, or someone deleted it.
            ?next_state(?s_create_subscription, Data);
        {done, _, {aborted, Details}} ->
            %% "The request raced with another user request. Please try again."
            %% when multiple connectors attempt this request in a cluster, this might
            %% happen.
            Msg = append_error_detail(~"Request aborted", Details),
            ?tp(debug, "gcp_pubsub_consumer_grpc_aborted_subscription_update", #{
                source_res_id => SourceResId,
                details => Msg
            }),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {done, _, Error} ->
            handle_update_subscription_error(Error, Data);
        {more, [#{name := _}]} ->
            ?next_state(?s_pull, Data);
        {more, Resp} ->
            %% impossible for this call
            Error = {error, {unexpected_update_subscription_response, Resp}},
            handle_update_subscription_error(Error, Data)
    end;
handle_update_subscription_response(Error, Data0) ->
    {#handle{}, Data} = maps_swap(?handle, ?undefined, Data0),
    %% no need to cancel the stream; it already closed.
    handle_update_subscription_error(Error, Data).

-spec handle_update_subscription_error(any(), data()) -> event_handler_result().
handle_update_subscription_error(Error, Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    ?tp(warning, "gcp_pubsub_consumer_failed_to_update_subscription", #{
        source_res_id => SourceResId,
        reason => Error
    }),
    case Error of
        {unauthenticated, _} ->
            %% impossible, since token is automatically refreshed a lot earlier than
            %% expiration?  just retry.
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {permission_denied, Details} ->
            %% unhealthy
            Msg = append_error_detail(~"Permission denied", Details),
            set_health(SourceResId, {?status_disconnected, {unhealthy_target, Msg}}),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {deadline_exceeded, _} ->
            %% retry; though user should likely tune timeouts.
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {invalid_argument, _} ->
            %% bug; no use in retrying.
            ?keep_state_and_data;
        {error, {grpc_client_down, _}} ->
            {_, Data1} = maps_swap(?handle, ?undefined, Data0),
            ?keep_state_actions(
                Data1,
                [?state_timeout(_Now = 0, #retry_subscription{})]
            );
        {error, {failed_to_get_token, _} = Reason} ->
            set_health(SourceResId, {?status_disconnected, Reason}),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        _ ->
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            )
    end.

%%==========================
%% Create sub
%%==========================

-spec handle_enter_create_subscription(data()) -> gen_statem:state_enter_result(state(), data()).
handle_enter_create_subscription(Data0) ->
    Data1 = Data0#{?handle := ?undefined},
    do_create_subscription(Data1).

-spec do_create_subscription(data()) -> event_handler_result().
do_create_subscription(Data0) ->
    Opts = grpc_opts(Data0),
    Req = create_subscription_req(Data0),
    maybe
        {ok, Metadata} ?= grpc_meta(Data0),
        {ok, Stream} ?= do_create_subscription_impl(Metadata, Opts),
        ok ?= grpc_send(Stream, Req, fin, Opts),
        Handle0 = grpc_client:async_install_receiver(Stream, #{mode => once}),
        Handle = #handle{handle = Handle0, stream = Stream},
        Data = Data0#{?handle := Handle},
        Timeout = req_timeout(Data),
        ?keep_state_actions(Data, [?state_timeout(Timeout, Handle)])
    else
        {error, Reason} ->
            handle_create_subscription_error(Reason, Data0)
    end.

-spec handle_create_subscription_response({ok, _} | {error, _}, data()) -> event_handler_result().
handle_create_subscription_response({ok, ResRaw}, Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    {#handle{stream = Stream}, Data} = maps_swap(?handle, ?undefined, Data0),
    %% no need to cancel the stream; it already closed (non-streaming).
    case map_grpc_reply(ResRaw, Stream) of
        {done, [#{name := _}], {ok, _}} ->
            ?next_state(?s_pull, Data);
        {done, _, {aborted, Details}} ->
            %% "The request raced with another user request. Please try again."
            %% when multiple connectors attempt this request in a cluster, this might
            %% happen.
            Msg = append_error_detail(~"Request aborted", Details),
            ?tp(debug, "gcp_pubsub_consumer_grpc_aborted_subscription_creation", #{
                source_res_id => SourceResId,
                details => Msg
            }),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {done, _, Error} ->
            handle_create_subscription_error(Error, Data);
        {more, [#{name := _}]} ->
            ?next_state(?s_pull, Data);
        {more, Resp} ->
            %% impossible for this call
            Error = {error, {unexpected_create_subscription_response, Resp}},
            handle_create_subscription_error(Error, Data)
    end;
handle_create_subscription_response(Error, Data0) ->
    {#handle{}, Data} = maps_swap(?handle, ?undefined, Data0),
    %% no need to cancel the stream; it already closed.
    handle_create_subscription_error(Error, Data).

-spec handle_create_subscription_timeout(data()) -> event_handler_result().
handle_create_subscription_timeout(Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    ?tp(warning, "gcp_pubsub_consumer_create_subscription_timeout", #{
        source_res_id => SourceResId
    }),
    {#handle{stream = Stream}, Data} = maps_swap(?handle, ?undefined, Data0),
    grpc_client:close_async(Stream),
    ?repeat_state(Data).

-spec handle_create_subscription_error(any(), data()) -> event_handler_result().
handle_create_subscription_error(Error, Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    ?tp(warning, "gcp_pubsub_consumer_failed_to_create_subscription", #{
        source_res_id => SourceResId,
        reason => Error
    }),
    case Error of
        {already_exists, _} ->
            %% race?  continue.
            ?next_state(?s_pull, Data0);
        {unauthenticated, _} ->
            %% impossible, since token is automatically refreshed a lot earlier than
            %% expiration?  just retry.
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {permission_denied, Details} ->
            %% unhealthy
            Msg = append_error_detail(~"Permission denied", Details),
            set_health(SourceResId, {?status_disconnected, {unhealthy_target, Msg}}),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {not_found, Details} ->
            %% unhealthy
            Msg = append_error_detail(~"Topic not found", Details),
            set_health(SourceResId, {?status_disconnected, {unhealthy_target, Msg}}),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {deadline_exceeded, _} ->
            %% retry; though user should likely tune timeouts.
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        {invalid_argument, _} ->
            %% bug; no use in retrying.
            ?keep_state_and_data;
        {error, {grpc_client_down, _}} ->
            {_, Data1} = maps_swap(?handle, ?undefined, Data0),
            ?keep_state_actions(
                Data1,
                [?state_timeout(_Now = 0, #retry_subscription{})]
            );
        {error, {failed_to_get_token, _} = Reason} ->
            set_health(SourceResId, {?status_disconnected, Reason}),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            );
        _ ->
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_SUB_TIMEOUT, #retry_subscription{})]
            )
    end.

%%==========================
%% Pull
%%==========================

-spec handle_enter_pull(data()) -> gen_statem:state_enter_result(state(), data()).
handle_enter_pull(Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    Data1 = Data0#{?handle := ?undefined},
    set_health(SourceResId, ?status_connected),
    do_pull(Data1).

-spec do_pull(data()) -> event_handler_result().
do_pull(Data0) ->
    Opts0 = grpc_opts(Data0),
    Opts = Opts0#{timeout => infinity},
    Req = streaming_pull_req(Data0),
    maybe
        {ok, Metadata} ?= grpc_meta(Data0),
        {ok, Stream} ?= do_streaming_pull_impl(Metadata, Opts),
        Data1 = pull_recv_async_active(Stream, Data0),
        ok ?= grpc_send(Stream, Req, Opts),
        Data = ack_pending(Data1),
        ?tp("gcp_pubsub_consumer_grpc_worker_pulling", #{}),
        ?keep_state(Data)
    else
        {error, Reason} ->
            handle_pull_error(Reason, Data0)
    end.

-spec handle_pull_error(term(), data()) -> event_handler_result().
handle_pull_error(Reason, Data0) ->
    #{?source_res_id := SourceResId} = Data0,
    ?tp(warning, "gcp_pubsub_consumer_grpc_failed_to_pull", #{
        source_res_id => SourceResId,
        reason => Reason
    }),
    case Reason of
        {not_found, _Details} ->
            %% race?  user deleted subscription as we were pulling.
            %% Ack ids are likely invalidated since it's gone.
            set_health(SourceResId, {?status_connecting, ~"Subscription gone while pulling"}),
            Data1 = Data0#{?pending_acks := []},
            ?next_state(?s_create_subscription, Data1);
        {unauthenticated, _} ->
            %% impossible, since token is automatically refreshed a lot earlier than
            %% expiration?  just retry.
            ?repeat_state(Data0);
        {invalid_argument, Details} ->
            %% bug; should not happen.
            Msg = append_error_detail(~"Invalid argument", Details),
            ?tp(warning, "gcp_pubsub_consumer_grpc_pull_error", #{
                source_res_id => SourceResId,
                details => Msg
            }),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_PULL_TIMEOUT, #retry_pull{})]
            );
        {permission_denied, Details} ->
            %% unhealthy
            Msg = append_error_detail(~"Permission denied", Details),
            set_health(SourceResId, {?status_disconnected, {unhealthy_target, Msg}}),
            {_, Data1} = maps_swap(?handle, ?undefined, Data0),
            ?keep_state_actions(
                Data1,
                [?state_timeout(?RETRY_PULL_TIMEOUT, #retry_pull{})]
            );
        {error, {grpc_client_down, _}} ->
            {_, Data1} = maps_swap(?handle, ?undefined, Data0),
            ?keep_state_actions(
                Data1,
                [?state_timeout(_Now = 0, #retry_pull{})]
            );
        {error, {failed_to_get_token, _} = Reason1} ->
            set_health(SourceResId, {?status_disconnected, Reason1}),
            ?keep_state_actions(
                Data0,
                [?state_timeout(?RETRY_PULL_TIMEOUT, #retry_pull{})]
            );
        _ ->
            {_, Data1} = maps_swap(?handle, ?undefined, Data0),
            ?keep_state_actions(
                Data1,
                [?state_timeout(?RETRY_PULL_TIMEOUT, #retry_pull{})]
            )
    end.

-spec handle_pull_response(term(), data()) -> event_handler_result().
handle_pull_response({ok, Res0}, Data0) ->
    #{?handle := #handle{stream = Stream}} = Data0,
    Res = map_grpc_reply(Res0, Stream),
    case Res of
        {done, Msgs0, Trailers} ->
            Data1 = process_msgs(Msgs0, Data0),
            {_, Data2} = maps_swap(?handle, ?undefined, Data1),
            handle_pull_response_trailers(Trailers, Data2);
        {more, Msgs0} ->
            Data1 = process_msgs(Msgs0, Data0),
            Data = ack_pending(Data1),
            ?keep_state(Data)
    end;
handle_pull_response({error, {connection_down, normal}}, Data0) ->
    %% every hour or so, gun processes seem to go down with this reason, at least when
    %% connected to real gcp.  no need to log.
    ?repeat_state(Data0);
handle_pull_response(Error, Data0) ->
    handle_pull_error(Error, Data0).

-spec handle_pull_response_trailers(term(), data()) -> event_handler_result().
handle_pull_response_trailers(Trailers, Data0) ->
    case Trailers of
        {unavailable, _} ->
            %% "The service was unable to fulfill your request. Please try
            %% again. [code=8a75]"
            %% gcp periodically closes the stream with this.
            ?repeat_state(Data0);
        _ ->
            handle_pull_error(Trailers, Data0)
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

maps_swap(K, V, M) ->
    #{K := OldV} = M,
    {OldV, M#{K := V}}.

set_health(SourceResId, HealthStatus) ->
    emqx_bridge_gcp_pubsub_consumer_grpc_impl:set_health(SourceResId, HealthStatus).

append_error_detail(Msg, ~"") ->
    Msg;
append_error_detail(Msg, Details) when is_binary(Details) ->
    <<Msg/binary, ": ", Details/binary>>;
append_error_detail(Msg, _Details) ->
    Msg.

pull_recv_async_active(Stream, Data0) ->
    Handle0 = grpc_client:async_install_receiver(Stream, #{mode => active}),
    Handle = #handle{handle = Handle0, stream = Stream},
    Data0#{?handle := Handle}.

%% for updating/creating subscriptions; pulling has no timeout.
req_timeout(#{?request_ttl := infinity}) ->
    ?DEFAULT_SUB_REQ_TIMEOUT;
req_timeout(#{?request_ttl := TTL}) ->
    TTL.

ack_pending(#{?pending_acks := []} = Data0) ->
    Data0;
ack_pending(#{?pending_acks := AckIds} = Data0) ->
    #{
        ?source_res_id := SourceResId,
        ?handle := #handle{stream = Stream}
    } = Data0,
    Opts = grpc_opts(Data0),
    Req = ack_streaming_pull_req(AckIds),
    ?tp("gcp_pubsub_consumer_grpc_will_send_ack_req0", #{}),
    ?tp("gcp_pubsub_consumer_grpc_will_send_ack_req1", #{}),
    case grpc_send(Stream, Req, Opts) of
        ok ->
            Data0#{?pending_acks := []};
        Error ->
            ?tp(warning, "gcp_pubsub_consumer_grpc_ack_req_error", #{
                source_res_id => SourceResId,
                reason => Error
            }),
            Data0
    end.

process_msgs([] = _Msgs, Data0) ->
    Data0;
process_msgs(Msgs0, Data0) ->
    #{?pending_acks := PendingAcks0} = Data0,
    AckIds = lists:flatmap(
        fun(#{received_messages := Msgs1}) ->
            lists:map(fun(M) -> process_msg(M, Data0) end, Msgs1)
        end,
        Msgs0
    ),
    Data0#{?pending_acks := PendingAcks0 ++ AckIds}.

process_msg(#{ack_id := AckId, message := Msg}, Data0) ->
    #{
        ?hookpoints := Hookpoints,
        ?topic_resource := TopicResource,
        ?namespace := Namespace,
        ?source_res_id := SourceResId
    } = Data0,
    #{
        attributes := Attributes,
        data := Data,
        message_id := MsgId,
        ordering_key := OrderingKey,
        publish_time := PublishTime
    } = Msg,
    FullMsg = #{
        attributes => Attributes,
        value => Data,
        ordering_key => OrderingKey,
        message_id => MsgId,
        %% n.b.: this publish time is of the form `#{seconds := _, nanos := _}` and not a
        %% string, differently from the corresponding rest api response.
        publish_time => PublishTime,
        topic => TopicResource
    },
    ?tp_span(
        "gcp_pubsub_consumer_grpc_worker_process_msg",
        #{},
        begin
            lists:foreach(
                fun(Hookpoint) -> emqx_hooks:run(Hookpoint, [FullMsg, Namespace]) end,
                Hookpoints
            ),
            emqx_resource_metrics:received_inc(SourceResId),
            AckId
        end
    ).

map_grpc_reply(Res0, Stream) ->
    Res1 = grpc_client:map_recv_async_reply(Stream, Res0),
    case is_end_of_stream(Res1) of
        {true, Results0, Trailers0} ->
            Trailers = grpc_client:trailers_to_error(Trailers0),
            {done, Results0, Trailers};
        false ->
            {more, Res1}
    end.

update_subscription_req(Data) ->
    #{
        subscription => create_subscription_req(Data),
        update_mask => #{paths => [~"ack_deadline_seconds"]}
    }.

create_subscription_req(Data) ->
    #{
        ?ack_deadline := AckDeadline,
        ?topic_resource := TopicResource,
        ?subscription_resource := SubscriptionResource
    } = Data,
    %% remember to update `update_mask` in `update_subscription_req` if we ever start
    %% using other fields here.
    #{
        name => SubscriptionResource,
        topic => TopicResource,
        ack_deadline_seconds => AckDeadline
    }.

streaming_pull_req(Data) ->
    #{
        ?subscription_resource := SubscriptionResource,
        ?ack_deadline := AckDeadline,
        ?max_outstanding_messages := MaxOutstandingMsgs
    } = Data,
    #{
        subscription => SubscriptionResource,
        stream_ack_deadline_seconds => AckDeadline,
        max_outstanding_messages => MaxOutstandingMsgs,
        client_id => client_id(Data)
    }.

ack_streaming_pull_req(AckIds) ->
    #{
        ack_ids => AckIds
    }.

client_id(Data) ->
    #{?source_res_id := SourceResId, ?idx := Idx} = Data,
    IdxBin = integer_to_binary(Idx),
    NodeBin = atom_to_binary(node(), utf8),
    <<NodeBin/binary, ":", IdxBin/binary, ":", SourceResId/binary>>.

-spec grpc_meta(data()) -> {ok, map()} | {error, {failed_to_get_token, any()}}.
grpc_meta(Data) ->
    #{?auth_ctx := #{auth_config := AuthCtx}} = Data,
    maybe
        {ok, Token} ?=
            emqx_bridge_gcp_pubsub_client:get_authorization_token_safe(AuthCtx),
        Meta = #{~"authorization" => <<"Bearer ", Token/binary>>},
        {ok, Meta}
    else
        {error, Reason} ->
            {error, {failed_to_get_token, Reason}}
    end.

grpc_opts(Data) ->
    #{
        ?client_pool := ClientPool,
        ?request_ttl := RequestTTL
    } = Data,
    #{
        channel => ClientPool,
        timeout => RequestTTL,
        content_type => ~"application/grpc"
    }.

grpc_send(Stream, Req, Opts) ->
    grpc_send(Stream, Req, nofin, Opts).

grpc_send(Stream, Req, FinOrNoFin, Opts) ->
    try
        grpc_client:send(Stream, Req, FinOrNoFin, Opts)
    catch
        error:Reason ->
            {error, Reason};
        Kind:Reason:Stacktrace ->
            {error, {Kind, Reason, Stacktrace}}
    end.

is_end_of_stream(Resp) ->
    maybe
        {value, {eos, Trailers}, Rest} ?= lists:keytake(eos, 1, Resp),
        {true, Rest, Trailers}
    end.

do_grpc_open_impl(Def, Metadata, Opts) ->
    try
        grpc_client:open(Def, Metadata, Opts)
    catch
        exit:noproc ->
            %% race: client died just as we called it
            {error, grpc_client_restarting};
        Class:Reason:Stacktrace ->
            {error, {Class, Reason, Stacktrace}}
    end.

do_streaming_pull_impl(Metadata, Opts) ->
    do_grpc_open_impl(
        ?DEF(
            <<"/google.pubsub.v1.Subscriber/StreamingPull">>,
            'google.pubsub.v1.streaming_pull_request',
            'google.pubsub.v1.streaming_pull_response',
            <<"google.pubsub.v1.StreamingPullRequest">>
        ),
        Metadata,
        Opts
    ).

do_update_subscription_impl(Metadata, Opts) ->
    do_grpc_open_impl(
        ?DEF(
            <<"/google.pubsub.v1.Subscriber/UpdateSubscription">>,
            'google.pubsub.v1.update_subscription_request',
            'google.pubsub.v1.subscription',
            <<"google.pubsub.v1.UpdateSubscriptionRequest">>
        ),
        Metadata,
        Opts
    ).

do_create_subscription_impl(Metadata, Opts) ->
    do_grpc_open_impl(
        ?DEF(
            <<"/google.pubsub.v1.Subscriber/CreateSubscription">>,
            'google.pubsub.v1.subscription',
            'google.pubsub.v1.subscription',
            <<"google.pubsub.v1.Subscription">>
        ),
        Metadata,
        Opts
    ).
