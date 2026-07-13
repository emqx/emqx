%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sync_request).

-behaviour(gen_server).

-include("emqx_sync_request.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_external_trace.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%% API
-export([
    start_link/0,
    child_spec/0,
    install_api_dispatch/0,
    uninstall_api_dispatch/0,
    request/1,
    status/0
]).

%% Plugin callbacks
-export([
    on_config_changed/1,
    on_health_check/0
]).

%% Hook callbacks
-export([
    on_message_delivered/2,
    on_message_publish/1
]).

%% Internal RPC targets
-export([
    cleanup_remote_pending/1,
    complete_remote_request/2,
    dispatch_remote_request/1,
    is_request_inflight/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(TIMEOUT, 15000).
-define(CONFIG_PT, {?MODULE, config}).
-define(API_MODULE, emqx_sync_request_api).
-define(MAX_REQUEST_ID_BYTES, 128).
-define(METRICS, [
    'sync_request.requests.total',
    'sync_request.requests.succeeded',
    'sync_request.requests.failed',
    'sync_request.requests.bad_request',
    'sync_request.requests.no_subscribers',
    'sync_request.requests.conflict',
    'sync_request.requests.too_many_requests',
    'sync_request.requests.dispatch_failed',
    'sync_request.requests.timeout',
    'sync_request.requests.internal_error'
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVICE}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVICE,
        start => {?MODULE, start_link, []}
    }.

request(Body) ->
    record_request_result(do_request_body(Body)).

do_request_body(Body) when is_map(Body) ->
    Config = config(),
    case parse_request(Body, Config) of
        {ok, Req} ->
            do_request(Req, Config);
        {error, Reason} ->
            {400, error_body(?CODE_BAD_REQUEST, Reason)}
    end;
do_request_body(_Body) ->
    {400, error_body(?CODE_BAD_REQUEST, <<"invalid_request_body">>)}.

status() ->
    #{
        requests_total => metric('sync_request.requests.total'),
        requests_succeeded => metric('sync_request.requests.succeeded'),
        requests_failed => metric('sync_request.requests.failed'),
        requests_bad_request => metric('sync_request.requests.bad_request'),
        requests_no_subscribers => metric('sync_request.requests.no_subscribers'),
        requests_conflict => metric('sync_request.requests.conflict'),
        requests_too_many_requests => metric('sync_request.requests.too_many_requests'),
        requests_dispatch_failed => metric('sync_request.requests.dispatch_failed'),
        requests_timeout => metric('sync_request.requests.timeout'),
        requests_internal_error => metric('sync_request.requests.internal_error'),
        inflight_requests => table_size(?REQ_TAB),
        pending_responses => table_size(?PENDING_TAB)
    }.

current_config() ->
    maps:merge(default_config(), emqx_plugins:get_config(name_vsn(), #{})).

install_api_dispatch() ->
    update_api_dispatch(add).

uninstall_api_dispatch() ->
    update_api_dispatch(remove).

update_api_dispatch(Action) ->
    try
        #{started := Listeners} = emqx_dashboard:listeners_status(),
        lists:foreach(fun(Name) -> update_listener_api_dispatch(Name, Action) end, Listeners),
        ok
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "sync_request_update_api_dispatch_failed",
                action => Action,
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.

%%--------------------------------------------------------------------
%% EMQX Plugin callbacks
%%--------------------------------------------------------------------

on_config_changed(NewConf) ->
    call({on_config_changed, NewConf}, ok).

on_health_check() ->
    call(on_health_check, {error, <<"Plugin is not running">>}).

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

on_message_delivered(_ClientInfo, Message = #message{headers = Headers}) ->
    case maps:get(?HEADER, Headers, undefined) of
        #{req_ref := ReqRef} ->
            maybe_register_pending(ReqRef, Message);
        _ ->
            ok
    end,
    {ok, Message}.

on_message_publish(Message = #message{headers = Headers}) ->
    case maps:is_key(?HEADER, Headers) of
        true ->
            ok;
        false ->
            maybe_complete_response(Message)
    end,
    {ok, Message}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = ensure_tables(),
    ok = init_metrics(),
    Config = normalize_config(current_config()),
    persistent_term:put(?CONFIG_PT, Config),
    ok = hook(),
    {ok, empty_state()}.

handle_call({on_config_changed, NewConf}, _From, State) ->
    Config = normalize_config(maps:merge(default_config(), NewConf)),
    persistent_term:put(?CONFIG_PT, Config),
    {reply, ok, State};
handle_call(on_health_check, _From, State) ->
    {reply, ok, State};
handle_call({monitor_waiter, ReqRef, Waiter}, _From, State = #{monitors := Mons}) ->
    Mon = erlang:monitor(process, Waiter),
    {reply, Mon, State#{monitors => Mons#{Mon => ReqRef}}};
handle_call(Request, From, State) ->
    ?SLOG(error, #{msg => "sync_request_unexpected_call", request => Request, from => From}),
    {reply, {error, unexpected_call}, State}.

handle_cast({forget_monitor, Mon}, State = #{monitors := Mons}) ->
    _ = erlang:demonitor(Mon, [flush]),
    {noreply, State#{monitors => maps:remove(Mon, Mons)}};
handle_cast(Request, State) ->
    ?SLOG(error, #{msg => "sync_request_unexpected_cast", request => Request}),
    {noreply, State}.

handle_info({'DOWN', Mon, process, _Pid, _Reason}, State = #{monitors := Mons}) ->
    case maps:take(Mon, Mons) of
        {ReqRef, Mons1} ->
            cleanup_request_tables(ReqRef),
            {noreply, State#{monitors => Mons1}};
        error ->
            {noreply, State}
    end;
handle_info({expire_pending, ReqRef}, State) ->
    cleanup_pending(ReqRef),
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "sync_request_unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = unhook(),
    persistent_term:erase(?CONFIG_PT),
    ok.

empty_state() ->
    #{monitors => #{}}.

%%--------------------------------------------------------------------
%% Request execution
%%--------------------------------------------------------------------

do_request(Req0, Config) ->
    MaxInflight = maps:get(max_inflight_requests, Config),
    case ets:info(?REQ_TAB, size) >= MaxInflight of
        true ->
            {429, error_body(?CODE_TOO_MANY_REQUESTS, <<"too_many_inflight_requests">>)};
        false ->
            ReqRef = make_ref(),
            TimeoutMs = maps:get(timeout, Req0),
            Deadline = now_ms() + TimeoutMs,
            Waiter = self(),
            Mon = monitor_waiter(ReqRef, Waiter),
            Req = Req0#{
                req_ref => ReqRef,
                waiter => Waiter,
                mon => Mon
            },
            true = ets:insert_new(?REQ_TAB, {ReqRef, Req}),
            Message = make_request_message(Req),
            dispatch_request(Message, ReqRef, Deadline)
    end.

dispatch_request(Message0 = #message{topic = Topic}, ReqRef, Deadline) ->
    case remaining_ms(Deadline) of
        Remaining when Remaining =< 0 ->
            cleanup_request(ReqRef),
            {504, error_body(?CODE_TIMEOUT, <<"timeout">>)};
        Remaining ->
            %% Refresh the relative timeout just before dispatch so the deliver
            %% node schedules pending expiry against remaining wait budget.
            Message = set_request_timeout(Message0, Remaining),
            case exact_route_node(Topic) of
                no_subscribers ->
                    cleanup_request(ReqRef),
                    {404, error_body(?CODE_NO_SUBSCRIBERS, <<"no_subscribers">>)};
                multiple_subscribers ->
                    cleanup_request(ReqRef),
                    {409, error_body(?CODE_CONFLICT, <<"multiple_subscribers">>)};
                {ok, Node} ->
                    case dispatch_to_node(Node, Message, Remaining) of
                        ok ->
                            wait_for_response(ReqRef, remaining_ms(Deadline));
                        no_subscribers ->
                            cleanup_request(ReqRef),
                            {404, error_body(?CODE_NO_SUBSCRIBERS, <<"no_subscribers">>)};
                        multiple_subscribers ->
                            cleanup_request(ReqRef),
                            {409, error_body(?CODE_CONFLICT, <<"multiple_subscribers">>)};
                        {error, timeout} ->
                            cleanup_request(ReqRef),
                            {504, error_body(?CODE_TIMEOUT, <<"timeout">>)};
                        {error, Reason} ->
                            cleanup_request(ReqRef),
                            ?SLOG(warning, #{
                                msg => "sync_request_dispatch_failed",
                                reason => Reason
                            }),
                            {503,
                                error_body(
                                    ?CODE_SERVICE_UNAVAILABLE, <<"failed_to_dispatch_request">>
                                )}
                    end
            end
    end.

exact_route_node(Topic) ->
    Routes = emqx_router:lookup_routes(Topic),
    case lists:any(fun is_shared_route/1, Routes) of
        true ->
            multiple_subscribers;
        false ->
            Nodes = lists:usort([
                Node
             || #route{dest = Node} <- Routes,
                is_atom(Node),
                emqx_router_helper:is_routable(Node)
            ]),
            case Nodes of
                [] -> no_subscribers;
                [Node] -> {ok, Node};
                [_ | _] -> multiple_subscribers
            end
    end.

is_shared_route(#route{dest = {_Group, _Node}}) ->
    true;
is_shared_route(_) ->
    false.

dispatch_to_node(Node, Message, _TimeoutMs) when Node =:= node() ->
    dispatch_local_request(Message);
dispatch_to_node(_Node, _Message, TimeoutMs) when TimeoutMs =< 0 ->
    {error, timeout};
dispatch_to_node(Node, Message, TimeoutMs) ->
    try erpc:call(Node, ?MODULE, dispatch_remote_request, [Message], TimeoutMs) of
        Result ->
            Result
    catch
        error:{erpc, timeout} ->
            {error, timeout};
        Class:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "sync_request_remote_dispatch_failed",
                node => Node,
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, Reason}
    end.

dispatch_remote_request(Message) ->
    dispatch_local_request(Message).

dispatch_local_request(#message{topic = Topic} = Message) ->
    case local_subscribers(Topic) of
        [] ->
            no_subscribers;
        [SubPid] ->
            SubPid ! {deliver, Topic, Message},
            ok;
        [_ | _] ->
            multiple_subscribers
    end.

local_subscribers(Topic) ->
    lists:flatmap(
        fun
            (SubPid) when is_pid(SubPid) ->
                case is_process_alive(SubPid) of
                    true -> [SubPid];
                    false -> []
                end;
            ({shard, I}) ->
                [
                    SubPid
                 || SubPid <- emqx_broker:subscribers({shard, Topic, I}),
                    is_pid(SubPid),
                    is_process_alive(SubPid)
                ];
            (_) ->
                []
        end,
        emqx_broker:subscribers(Topic)
    ).

wait_for_response(ReqRef, TimeoutMs) when TimeoutMs =< 0 ->
    cleanup_request(ReqRef),
    {504, error_body(?CODE_TIMEOUT, <<"timeout">>)};
wait_for_response(ReqRef, TimeoutMs) ->
    receive
        {emqx_sync_request_response, ReqRef, {ok, Response}} ->
            {200, #{code => ?CODE_OK, message => ?CODE_OK, response => Response}};
        {emqx_sync_request_response, ReqRef, {error, StatusCode, Reason}} ->
            {StatusCode, error_body(code_for_http_error(StatusCode), Reason)};
        {emqx_sync_request_response, ReqRef, {error, Reason}} ->
            {500, error_body(?CODE_INTERNAL_ERROR, Reason)}
    after TimeoutMs ->
        cleanup_request(ReqRef),
        {504, error_body(?CODE_TIMEOUT, <<"timeout">>)}
    end.

make_request_message(Req) ->
    Props0 = #{
        'Response-Topic' => maps:get(response_topic, Req),
        'Correlation-Data' => maps:get(correlation_data, Req)
    },
    Props = maybe_put('Content-Type', maps:get(content_type, Req, undefined), Props0),
    Headers = #{
        properties => Props,
        ?HEADER => #{
            req_ref => maps:get(req_ref, Req),
            timeout => maps:get(timeout, Req)
        }
    },
    emqx_message:make(
        ?EXT_TRACE__HTTP_API_INTERNAL_CLIENTID,
        maps:get(qos, Req),
        maps:get(topic, Req),
        maps:get(payload, Req),
        #{retain => false},
        Headers
    ).

set_request_timeout(Message = #message{headers = Headers0}, TimeoutMs) ->
    case maps:get(?HEADER, Headers0, undefined) of
        Meta when is_map(Meta) ->
            Message#message{
                headers = Headers0#{?HEADER => Meta#{timeout => TimeoutMs}}
            };
        _ ->
            Message
    end.

%%--------------------------------------------------------------------
%% Pending registry
%%--------------------------------------------------------------------

maybe_register_pending(
    ReqRef,
    #message{headers = #{properties := Props, ?HEADER := #{timeout := TimeoutMs}}}
) ->
    ResponseTopic = maps:get('Response-Topic', Props, undefined),
    CorrelationData = maps:get('Correlation-Data', Props, undefined),
    case ResponseTopic =/= undefined andalso should_register_pending(ReqRef) of
        true ->
            %% Deadline is node-local monotonic time; timeout is relative and
            %% recomputed when the request is delivered on this node.
            Deadline = now_ms() + TimeoutMs,
            Seq = erlang:unique_integer([monotonic, positive]),
            Pending = {ResponseTopic, Seq, ReqRef, CorrelationData, Deadline},
            true = ets:insert(?PENDING_BY_REQ_TAB, {ReqRef, Pending}),
            true = ets:insert(?PENDING_TAB, Pending),
            %% Fire-and-forget; cleanup_pending is idempotent if the request
            %% completes before the timer fires.
            _ = erlang:send_after(max(0, TimeoutMs), ?SERVICE, {expire_pending, ReqRef}),
            ok;
        false ->
            ok
    end;
maybe_register_pending(_ReqRef, _Message) ->
    ok.

%% Local origin: skip if the HTTP waiter already timed out / exited.
%% Remote origin: always register and rely on the expiry timer.
should_register_pending(ReqRef) when node(ReqRef) =:= node() ->
    is_request_inflight(ReqRef);
should_register_pending(_ReqRef) ->
    true.

is_request_inflight(ReqRef) ->
    ets:member(?REQ_TAB, ReqRef).

maybe_complete_response(Message = #message{topic = Topic, headers = Headers}) ->
    Props = maps:get(properties, Headers, #{}),
    Corr = maps:get('Correlation-Data', Props, undefined),
    Pending0 = ets:lookup(?PENDING_TAB, Topic),
    Pending = select_pending(Pending0, Corr),
    try_complete_pending(Pending, Message).

select_pending(Pending, undefined) ->
    sort_pending(Pending);
select_pending(Pending, Corr) ->
    sort_pending([P || P = {_Topic, _Seq, _ReqRef, Corr0, _Deadline} <- Pending, Corr0 =:= Corr]).

sort_pending(Pending) ->
    lists:keysort(2, Pending).

try_complete_pending([], _Message) ->
    ok;
try_complete_pending([Pending = {_Topic, _Seq, ReqRef, _Corr, Deadline} | Rest], Message) ->
    case expired(Deadline) of
        true ->
            cleanup_request(ReqRef),
            try_complete_pending(Rest, Message);
        false ->
            case complete_request(ReqRef, Message) of
                true ->
                    ok;
                false ->
                    delete_pending(Pending),
                    try_complete_pending(Rest, Message)
            end
    end.

complete_request(ReqRef, Message = #message{payload = Payload}) ->
    case node(ReqRef) =:= node() of
        true ->
            complete_local_request(ReqRef, Message#message{payload = iolist_to_binary(Payload)});
        false ->
            complete_request_on_origin_node(ReqRef, Message#message{
                payload = iolist_to_binary(Payload)
            })
    end.

complete_request_on_origin_node(ReqRef, Message) ->
    try erpc:call(node(ReqRef), ?MODULE, complete_remote_request, [ReqRef, Message], ?TIMEOUT) of
        true ->
            cleanup_pending(ReqRef),
            true;
        false ->
            false
    catch
        _:_ ->
            false
    end.

complete_remote_request(ReqRef, Message) ->
    complete_local_request(ReqRef, Message).

complete_local_request(ReqRef, Message = #message{payload = Payload}) ->
    case ets:take(?REQ_TAB, ReqRef) of
        [{ReqRef, Req}] ->
            forget_monitor(maps:get(mon, Req)),
            cleanup_pending(ReqRef),
            cleanup_pending_on_peer_nodes(ReqRef),
            Config = config(),
            case byte_size(Payload) =< maps:get(max_payload_size, Config) of
                true ->
                    Response = make_response(Req, Message),
                    maps:get(waiter, Req) ! {emqx_sync_request_response, ReqRef, {ok, Response}},
                    true;
                false ->
                    maps:get(waiter, Req) !
                        {emqx_sync_request_response, ReqRef,
                            {error, 400, <<"response_payload_too_large">>}},
                    true
            end;
        [] ->
            false
    end.

make_response(Req, #message{topic = Topic, payload = Payload, headers = Headers}) ->
    Props = maps:get(properties, Headers, #{}),
    Response0 = #{
        topic => Topic,
        request_id => maps:get(request_id, Req),
        payload_encoding => <<"base64">>,
        payload => base64:encode(Payload)
    },
    maybe_put(content_type, maps:get('Content-Type', Props, undefined), Response0).

cleanup_request(ReqRef) ->
    case ets:take(?REQ_TAB, ReqRef) of
        [{ReqRef, #{mon := Mon}}] ->
            forget_monitor(Mon);
        [{ReqRef, _Req}] ->
            ok;
        [] ->
            ok
    end,
    cleanup_request_tables(ReqRef).

%% Cleanup tables without touching waiter monitors (used on monitor DOWN).
cleanup_request_tables(ReqRef) ->
    ets:delete(?REQ_TAB, ReqRef),
    cleanup_pending(ReqRef),
    cleanup_pending_on_peer_nodes(ReqRef).

cleanup_pending(ReqRef) ->
    lists:foreach(
        fun({_ReqRef, Pending}) -> delete_pending(Pending) end,
        ets:lookup(?PENDING_BY_REQ_TAB, ReqRef)
    ),
    ets:match_delete(?PENDING_TAB, {'$1', '$2', ReqRef, '$3', '$4'}),
    ets:delete(?PENDING_BY_REQ_TAB, ReqRef),
    ok.

delete_pending(Pending = {_Topic, _Seq, ReqRef, _Corr, _Deadline}) ->
    ets:delete_object(?PENDING_TAB, Pending),
    ets:delete_object(?PENDING_BY_REQ_TAB, {ReqRef, Pending}).

cleanup_pending_on_peer_nodes(ReqRef) ->
    lists:foreach(
        fun(Node) -> _ = rpc:cast(Node, ?MODULE, cleanup_remote_pending, [ReqRef]) end,
        emqx:running_nodes() -- [node()]
    ),
    ok.

cleanup_remote_pending(ReqRef) ->
    cleanup_pending(ReqRef).

%%--------------------------------------------------------------------
%% Parsing and validation
%%--------------------------------------------------------------------

parse_request(Body, Config) ->
    try
        Request = required(request, Body),
        Timeout = parse_timeout(Body, Config),
        Payload = parse_payload(Request),
        MaxPayloadSize = maps:get(max_payload_size, Config),
        case byte_size(Payload) =< MaxPayloadSize of
            false ->
                throw({bad_request, <<"request_payload_too_large">>});
            true ->
                Topic = validate_topic(required(topic, Request)),
                ResponseTopic = validate_topic(required(response_topic, Request)),
                RequestId = parse_request_id(required(request_id, Request)),
                {ok, #{
                    timeout => Timeout,
                    topic => Topic,
                    response_topic => ResponseTopic,
                    request_id => RequestId,
                    correlation_data => RequestId,
                    qos => parse_qos(get(Request, qos, 0)),
                    payload => Payload,
                    content_type => optional_binary(content_type, Request)
                }}
        end
    catch
        throw:{bad_request, Reason} ->
            {error, Reason}
    end.

parse_timeout(Body, Config) ->
    TimeoutValue = get(Body, timeout, maps:get(default_timeout, Config)),
    Timeout = parse_duration_ms(TimeoutValue),
    MaxTimeout = maps:get(max_timeout, Config),
    case Timeout > 0 andalso Timeout =< MaxTimeout of
        true -> Timeout;
        false -> throw({bad_request, <<"invalid_timeout">>})
    end.

parse_payload(Request) ->
    Payload0 = required(payload, Request),
    Encoding = get(Request, payload_encoding, plain),
    case normalize_payload_encoding(Encoding) of
        plain ->
            to_binary(Payload0);
        base64 ->
            try base64:decode(Payload0) of
                Payload -> Payload
            catch
                _:_ -> throw({bad_request, <<"invalid_base64_payload">>})
            end;
        invalid ->
            throw({bad_request, <<"invalid_payload_encoding">>})
    end.

parse_qos(QoS) when is_integer(QoS), ?QOS_0 =< QoS, QoS =< ?QOS_2 ->
    QoS;
parse_qos(<<"0">>) ->
    ?QOS_0;
parse_qos(<<"1">>) ->
    ?QOS_1;
parse_qos(<<"2">>) ->
    ?QOS_2;
parse_qos(_) ->
    throw({bad_request, <<"invalid_qos">>}).

parse_request_id(RequestId0) ->
    RequestId = to_binary(RequestId0),
    case byte_size(RequestId) =< ?MAX_REQUEST_ID_BYTES of
        true -> RequestId;
        false -> throw({bad_request, <<"request_id_too_large">>})
    end.

validate_topic(Topic0) ->
    Topic = to_binary(Topic0),
    try
        true = emqx_topic:validate(name, Topic),
        Topic
    catch
        _:_ -> throw({bad_request, <<"invalid_topic">>})
    end.

optional_binary(Key, Map) ->
    case find(Key, Map) of
        {ok, Value} -> to_binary(Value);
        error -> undefined
    end.

required(Key, Map) ->
    case find(Key, Map) of
        {ok, Value} -> Value;
        error -> throw({bad_request, iolist_to_binary([atom_to_binary(Key), <<"_required">>])})
    end.

get(Map, Key, Default) ->
    case find(Key, Map) of
        {ok, Value} -> Value;
        error -> Default
    end.

find(Key, Map) when is_atom(Key) ->
    BinKey = atom_to_binary(Key),
    case maps:find(BinKey, Map) of
        {ok, Value} ->
            {ok, Value};
        error ->
            maps:find(Key, Map)
    end.

normalize_payload_encoding(plain) -> plain;
normalize_payload_encoding(<<"plain">>) -> plain;
normalize_payload_encoding(base64) -> base64;
normalize_payload_encoding(<<"base64">>) -> base64;
normalize_payload_encoding(_) -> invalid.

%%--------------------------------------------------------------------
%% Config and hooks
%%--------------------------------------------------------------------

default_config() ->
    #{
        <<"default_timeout">> => ?DEFAULT_TIMEOUT,
        <<"max_timeout">> => ?DEFAULT_MAX_TIMEOUT,
        <<"max_inflight_requests">> => ?DEFAULT_MAX_INFLIGHT,
        <<"max_payload_size">> => ?DEFAULT_MAX_PAYLOAD_SIZE
    }.

normalize_config(Config) ->
    #{
        default_timeout => parse_config_duration(
            get(Config, default_timeout, ?DEFAULT_TIMEOUT), ?DEFAULT_TIMEOUT
        ),
        max_timeout => parse_config_duration(
            get(Config, max_timeout, ?DEFAULT_MAX_TIMEOUT), ?DEFAULT_MAX_TIMEOUT
        ),
        max_inflight_requests =>
            parse_config_pos_integer(
                get(Config, max_inflight_requests, ?DEFAULT_MAX_INFLIGHT),
                ?DEFAULT_MAX_INFLIGHT
            ),
        max_payload_size =>
            parse_config_bytesize(
                get(Config, max_payload_size, ?DEFAULT_MAX_PAYLOAD_SIZE),
                ?DEFAULT_MAX_PAYLOAD_SIZE
            )
    }.

parse_config_duration(Value, Default) ->
    try parse_duration_ms(Value) of
        Ms when is_integer(Ms), Ms > 0 -> Ms;
        _ -> parse_duration_ms(Default)
    catch
        _:_ -> parse_duration_ms(Default)
    end.

parse_config_bytesize(Value, Default) ->
    try parse_bytesize(Value) of
        Bytes when is_integer(Bytes), Bytes > 0 -> Bytes;
        _ -> parse_bytesize(Default)
    catch
        _:_ -> parse_bytesize(Default)
    end.

parse_config_pos_integer(Value, _Default) when is_integer(Value), Value > 0 ->
    Value;
parse_config_pos_integer(Value, Default) ->
    try
        case binary_to_integer(to_binary(Value)) of
            I when I > 0 -> I;
            _ -> Default
        end
    catch
        _:_ -> Default
    end.

parse_duration_ms(Value) when is_integer(Value) ->
    Value;
parse_duration_ms(Value) ->
    case emqx_schema:to_duration_ms(to_binary(Value)) of
        {ok, Ms} -> Ms;
        {error, _} -> throw({bad_request, <<"invalid_duration">>})
    end.

parse_bytesize(Value) when is_integer(Value) ->
    Value;
parse_bytesize(Value) ->
    case emqx_schema:to_bytesize(to_binary(Value)) of
        {ok, Bytes} -> Bytes;
        {error, _} -> throw({bad_request, <<"invalid_bytesize">>})
    end.

config() ->
    persistent_term:get(?CONFIG_PT, normalize_config(default_config())).

init_metrics() ->
    lists:foreach(
        fun(Name) ->
            ok = emqx_metrics:ensure(Name),
            ok = emqx_metrics:set(Name, 0)
        end,
        ?METRICS
    ).

record_request_result(Result = {Status, _Body}) ->
    ok = emqx_metrics:inc('sync_request.requests.total'),
    ok =
        case Status of
            200 -> emqx_metrics:inc('sync_request.requests.succeeded');
            _ -> emqx_metrics:inc('sync_request.requests.failed')
        end,
    ok = maybe_inc_status_metric(Status),
    Result.

maybe_inc_status_metric(200) -> ok;
maybe_inc_status_metric(400) -> emqx_metrics:inc('sync_request.requests.bad_request');
maybe_inc_status_metric(404) -> emqx_metrics:inc('sync_request.requests.no_subscribers');
maybe_inc_status_metric(409) -> emqx_metrics:inc('sync_request.requests.conflict');
maybe_inc_status_metric(429) -> emqx_metrics:inc('sync_request.requests.too_many_requests');
maybe_inc_status_metric(503) -> emqx_metrics:inc('sync_request.requests.dispatch_failed');
maybe_inc_status_metric(504) -> emqx_metrics:inc('sync_request.requests.timeout');
maybe_inc_status_metric(500) -> emqx_metrics:inc('sync_request.requests.internal_error');
maybe_inc_status_metric(_) -> ok.

metric(Name) ->
    emqx_metrics:val(Name).

table_size(Tab) ->
    case ets:info(Tab, size) of
        undefined -> 0;
        Size -> Size
    end.

update_listener_api_dispatch(Name, Action) ->
    [Name, Transport, SocketOpts, Protocol, ProtoOpts0] = ranch_server:get_listener_start_args(
        Name
    ),
    #{env := Env0 = #{options := Options0}} = ProtoOpts0,
    Modules0 = maps:get(modules, Options0, []),
    Modules = update_api_modules(Action, Modules0),
    Options = Options0#{modules => Modules},
    ProtoOpts = ProtoOpts0#{env := Env0#{options := Options}},
    StartArgs = [Name, Transport, SocketOpts, Protocol, ProtoOpts],
    true = ets:insert(ranch_server, {{listener_start_args, Name}, StartArgs}),
    ok = minirest:update_dispatch(Name).

update_api_modules(add, Modules) ->
    [?API_MODULE | [Module || Module <- Modules, Module =/= ?API_MODULE]];
update_api_modules(remove, Modules) ->
    [Module || Module <- Modules, Module =/= ?API_MODULE].

hook() ->
    ok = emqx_hooks:put('message.delivered', {?MODULE, on_message_delivered, []}, ?HP_HIGHEST),
    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_HIGHEST).

unhook() ->
    ok = emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

ensure_tables() ->
    _ = ets:new(?REQ_TAB, [named_table, public, set, {read_concurrency, true}]),
    _ = ets:new(?PENDING_TAB, [named_table, public, duplicate_bag, {read_concurrency, true}]),
    _ = ets:new(?PENDING_BY_REQ_TAB, [named_table, public, duplicate_bag]),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

call(Request, Default) ->
    try gen_server:call(?SERVICE, Request, ?TIMEOUT) of
        Reply -> Reply
    catch
        exit:{noproc, _} -> Default;
        exit:{timeout, _} -> Default
    end.

monitor_waiter(ReqRef, Waiter) ->
    gen_server:call(?SERVICE, {monitor_waiter, ReqRef, Waiter}, ?TIMEOUT).

forget_monitor(Mon) ->
    try gen_server:cast(?SERVICE, {forget_monitor, Mon}) of
        ok -> ok
    catch
        _:_ -> ok
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(?PLUGIN_NAME, vsn),
    iolist_to_binary([atom_to_binary(?PLUGIN_NAME), <<"-">>, Vsn]).

expired(Deadline) ->
    now_ms() > Deadline.

remaining_ms(Deadline) ->
    Deadline - now_ms().

now_ms() ->
    erlang:monotonic_time(millisecond).

maybe_put(_Key, undefined, Map) ->
    Map;
maybe_put(Key, Value, Map) ->
    Map#{Key => Value}.

error_body(Code, Reason) ->
    #{
        code => Code,
        message => reason_hint(Reason)
    }.

code_for_http_error(400) -> ?CODE_BAD_REQUEST;
code_for_http_error(409) -> ?CODE_CONFLICT;
code_for_http_error(429) -> ?CODE_TOO_MANY_REQUESTS;
code_for_http_error(503) -> ?CODE_SERVICE_UNAVAILABLE;
code_for_http_error(_) -> ?CODE_INTERNAL_ERROR.

reason_hint(<<"invalid_request_body">>) ->
    <<"Request body must be a JSON object.">>;
reason_hint(<<"request_required">>) ->
    <<"request object is required.">>;
reason_hint(<<"topic_required">>) ->
    <<"request.topic is required.">>;
reason_hint(<<"response_topic_required">>) ->
    <<"request.response_topic is required.">>;
reason_hint(<<"payload_required">>) ->
    <<"request.payload is required.">>;
reason_hint(<<"request_id_required">>) ->
    <<"request.request_id is required.">>;
reason_hint(<<"invalid_topic">>) ->
    <<"Topic must be a valid MQTT topic name without wildcards.">>;
reason_hint(<<"invalid_qos">>) ->
    <<"request.qos must be 0, 1, or 2.">>;
reason_hint(<<"invalid_payload_encoding">>) ->
    <<"request.payload_encoding must be plain or base64.">>;
reason_hint(<<"invalid_base64_payload">>) ->
    <<"request.payload must be valid base64 when payload_encoding is base64.">>;
reason_hint(<<"invalid_duration">>) ->
    <<"timeout must be a valid duration.">>;
reason_hint(<<"invalid_timeout">>) ->
    <<"timeout must be greater than 0 and no more than max_timeout.">>;
reason_hint(<<"request_id_too_large">>) ->
    <<"request.request_id must be no longer than 128 bytes.">>;
reason_hint(<<"request_payload_too_large">>) ->
    <<"request.payload exceeds max_payload_size.">>;
reason_hint(<<"response_payload_too_large">>) ->
    <<"MQTT response payload exceeds max_payload_size.">>;
reason_hint(<<"too_many_inflight_requests">>) ->
    <<"Too many sync requests are waiting for responses.">>;
reason_hint(<<"no_subscribers">>) ->
    <<"No exact subscriber is online for the request topic.">>;
reason_hint(<<"multiple_subscribers">>) ->
    <<"The request topic has a shared subscription or more than one exact subscriber.">>;
reason_hint(<<"failed_to_dispatch_request">>) ->
    <<"Failed to dispatch the request to the subscriber node.">>;
reason_hint(<<"timeout">>) ->
    <<"Timed out waiting for a matching MQTT response.">>;
reason_hint(Reason) ->
    to_binary(Reason).

to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_atom(Value) ->
    atom_to_binary(Value);
to_binary(Value) when is_list(Value) ->
    iolist_to_binary(Value);
to_binary(Value) when is_integer(Value) ->
    integer_to_binary(Value);
to_binary(Value) ->
    iolist_to_binary(io_lib:format("~0p", [Value])).
