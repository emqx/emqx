%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigtable_client).

%% API
-export([
    start/2,
    stop/1,

    health_check/3,

    mutate_rows/5,
    ping_and_warm/4,

    async_recv/5
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_bridge_bigtable.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(SERVICE, 'google.bigtable.v2.Bigtable').
-define(PROTO_MODULE, 'emqx_bigtable_gen_bigtable_pb').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType), #{
    path => Path,
    service => ?SERVICE,
    message_type => MessageType,
    marshal => ?MARSHAL(Req),
    unmarshal => ?UNMARSHAL(Resp)
}).

-define(SUP, emqx_bridge_bigtable_sup).
%% ms
-define(DEFAULT_TIMEOUT, 5_000).

-define(grpc_client_name(RESID), {RESID, grpc_client_sup}).

-type instance_name() :: binary().
-type req_opts_in() :: #{
    connect_timeout => timeout(),
    timeout => timeout(),
    exit_on_connection_crash => boolean(),
    any() => term()
}.
-type state() :: #{
    project_id := binary(),
    recv_pool := term(),
    auth_config := term()
}.

-export_type([state/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start(ConnResId, Opts0) ->
    #{
        url := URL,
        authentication := _Authentication,
        pool_size := PoolSize
    } = Opts0,
    Opts = Opts0#{
        jwt_opts => #{
            %% trailing slash is important.
            aud => <<"https://bigtable.googleapis.com/">>
        },
        supervisor => ?SUP,
        token_table => ?TOKEN_TAB,
        sa_server_ref => ?SA_SERVER_REF,
        sa_token_table => ?SA_TOKEN_RESP_TAB
    },
    maybe
        {ok, AuthCtx} ?=
            emqx_bridge_gcp_pubsub_client:maybe_initialize_auth_resources(ConnResId, Opts),
        GRPCOpts = #{pool_size => PoolSize},
        {ok, #{recv_pool := Pool}} ?= ensure_channel_pool(ConnResId, URL, GRPCOpts),
        State = AuthCtx#{recv_pool => Pool},
        {ok, State}
    else
        {error, Reason} ->
            stop(ConnResId),
            {error, Reason}
    end.

stop(ConnResId) ->
    emqx_bridge_bigtable_client_sup:ensure_stopped(ConnResId),
    Ctx = #{
        supervisor => ?SUP,
        token_table => ?TOKEN_TAB,
        sa_server_ref => ?SA_SERVER_REF,
        sa_token_table => ?SA_TOKEN_RESP_TAB
    },
    emqx_bridge_gcp_pubsub_client:stop_auth_resources(ConnResId, Ctx),
    ok.

health_check(ConnResId, Opts, _State) ->
    Workers = grpc_client_sup:workers(?grpc_client_name(ConnResId)),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    ConnectTimeout = maps:get(connect_timeout, Opts, ?DEFAULT_TIMEOUT),
    Fn = fun({_Id, Worker}) ->
        Opts1 = #{connect_timeout => ConnectTimeout},
        grpc_client:health_check(Worker, Opts1)
    end,
    try emqx_utils:pmap(Fn, Workers, Timeout) of
        [] ->
            {?status_connecting, <<"empty_pool">>};
        Results ->
            Errors = lists:filter(fun(Res) -> Res /= ok end, Results),
            case Errors of
                [] ->
                    ?status_connected;
                [{error, Reason} | _] ->
                    {?status_disconnected, Reason};
                [Error | _] ->
                    {?status_disconnected, Error}
            end
    catch
        exit:timeout ->
            {?status_disconnected, timeout};
        Kind:Reason:Stacktrace ->
            {?status_disconnected, {Kind, Reason, Stacktrace}}
    end.

mutate_rows(ConnResId, InstanceName, Req, Opts, State) ->
    {Metadata, ReqOpts} = mk_metadata_and_opts(ConnResId, InstanceName, Opts, State),
    maybe
        {ok, Stream} ?= do_mutate_rows_impl(Metadata, ReqOpts),
        ok ?= grpc_send(Stream, Req, ReqOpts),
        {ok, Stream}
    end.

async_recv(Stream, Opts, ReplyFnAndArgs, Deadline, State) ->
    #{recv_pool := Pool} = State,
    emqx_bridge_bigtable_async_receiver_worker:async_recv(
        Pool, Stream, Opts, ReplyFnAndArgs, Deadline
    ).

ping_and_warm(ConnResId, InstanceName, Opts, State) ->
    {Metadata, ReqOpts} = mk_metadata_and_opts(ConnResId, InstanceName, Opts, State),
    Req = #{name => InstanceName},
    do_ping_and_warm_impl(Req, Metadata, ReqOpts).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec mk_metadata_and_opts(resource_id(), instance_name(), req_opts_in(), state()) ->
    {map(), map()}.
mk_metadata_and_opts(ConnResId, InstanceName, Opts, State) ->
    #{auth_config := AuthConfig} = State,
    Token = get_auth_token(AuthConfig),
    Metadata = #{
        <<"x-goog-request-params">> => <<"name=", InstanceName/binary, "&app_profile_id=">>,
        <<"authorization">> => <<"Bearer ", Token/binary>>
    },
    ConnectTimeout = maps:get(connect_timeout, Opts, ?DEFAULT_TIMEOUT),
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    ReqOpts = #{
        channel => ?grpc_client_name(ConnResId),
        content_type => <<"application/grpc">>,
        connect_timeout => ConnectTimeout,
        timeout => Timeout,
        exit_on_connection_crash => false
    },
    {Metadata, ReqOpts}.

do_mutate_rows_impl(Metadata, Opts) ->
    grpc_client:open(
        ?DEF(
            <<"/google.bigtable.v2.Bigtable/MutateRows">>,
            'google.bigtable.v2.mutate_rows_request',
            'google.bigtable.v2.mutate_rows_response',
            <<"google.bigtable.v2.MutateRowsRequest">>
        ),
        Metadata,
        Opts
    ).

do_ping_and_warm_impl(Req, Metadata, Opts) ->
    grpc_client:unary(
        ?DEF(
            <<"/google.bigtable.v2.Bigtable/PingAndWarm">>,
            'google.bigtable.v2.ping_and_warm_request',
            'google.bigtable.v2.ping_and_warm_response',
            <<"google.bigtable.v2.PingAndWarmRequest">>
        ),
        Req,
        Metadata,
        Opts
    ).

ensure_channel_pool(ConnResId, URL, GRPCOpts0) ->
    #{pool_size := PoolSize} = GRPCOpts0,
    GRPCOpts = GRPCOpts0#{retries => 0},
    Opts = #{
        url => URL,
        grpc_opts => GRPCOpts,
        pool_size => PoolSize
    },
    emqx_bridge_bigtable_client_sup:ensure_started(ConnResId, Opts).

get_auth_token(State) ->
    emqx_bridge_gcp_pubsub_client:get_authorization_token(State).

grpc_send(Stream, Req, Opts) ->
    try
        grpc_client:send(Stream, Req, nofin, Opts)
    catch
        error:Reason ->
            {error, Reason}
    end.
