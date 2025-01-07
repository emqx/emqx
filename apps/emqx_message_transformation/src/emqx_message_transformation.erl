%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation).

-feature(maybe_expr, enable).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([
    list/0,
    reorder/1,
    lookup/1,
    insert/1,
    update/1,
    delete/1
]).

%% `emqx_hooks' API
-export([
    register_hooks/0,
    unregister_hooks/0,

    on_message_publish/1
]).

%% Internal exports
-export([run_transformation/2, trace_failure_context_to_map/1, prettify_operation/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(TRACE_TAG, "MESSAGE_TRANSFORMATION").

-record(trace_failure_context, {
    transformation :: transformation(),
    tag :: string(),
    context :: map()
}).
-type trace_failure_context() :: #trace_failure_context{}.

-type transformation_name() :: binary().
%% TODO: make more specific typespec
-type transformation() :: #{atom() => term()}.
%% TODO: make more specific typespec
-type variform() :: any().
-type failure_action() :: ignore | drop | disconnect.
-type operation() :: #{key := [binary(), ...], value := variform()}.
-type qos() :: 0..2.
-type rendered_value() :: qos() | boolean() | binary().

-type eval_context() :: #{
    client_attrs := map(),
    clientid := _,
    flags := _,
    id := _,
    node := _,
    payload := _,
    peername := _,
    pub_props := _,
    publish_received_at := _,
    qos := _,
    retain := _,
    timestamp := _,
    topic := _,
    user_property := _,
    username := _,
    dirty := #{
        payload => true,
        qos => true,
        retain => true,
        topic => true,
        user_property => true
    }
}.

-export_type([
    transformation/0,
    transformation_name/0,
    failure_action/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec list() -> [transformation()].
list() ->
    emqx_message_transformation_config:list().

-spec reorder([transformation_name()]) ->
    {ok, _} | {error, _}.
reorder(Order) ->
    emqx_message_transformation_config:reorder(Order).

-spec lookup(transformation_name()) -> {ok, transformation()} | {error, not_found}.
lookup(Name) ->
    emqx_message_transformation_config:lookup(Name).

-spec insert(transformation()) ->
    {ok, _} | {error, _}.
insert(Transformation) ->
    emqx_message_transformation_config:insert(Transformation).

-spec update(transformation()) ->
    {ok, _} | {error, _}.
update(Transformation) ->
    emqx_message_transformation_config:update(Transformation).

-spec delete(transformation_name()) ->
    {ok, _} | {error, _}.
delete(Name) ->
    emqx_message_transformation_config:delete(Name).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    emqx_hooks:put(
        'message.publish', {?MODULE, on_message_publish, []}, ?HP_MESSAGE_TRANSFORMATION
    ).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

-spec on_message_publish(emqx_types:message()) ->
    {ok, emqx_types:message()} | {stop, emqx_types:message()}.
on_message_publish(Message = #message{topic = Topic}) ->
    case emqx_message_transformation_registry:matching_transformations(Topic) of
        [] ->
            ok;
        Transformations ->
            run_transformations(Transformations, Message)
    end.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

-spec run_transformation(transformation(), emqx_types:message()) ->
    {ok, emqx_types:message()} | {failure_action(), trace_failure_context()}.
run_transformation(Transformation, MessageIn) ->
    #{
        operations := Operations,
        failure_action := FailureAction,
        payload_decoder := PayloadDecoder
    } = Transformation,
    Fun = fun(Operation, Acc) ->
        case eval_operation(Operation, Transformation, Acc) of
            {ok, NewAcc} -> {cont, NewAcc};
            {error, TraceFailureContext} -> {halt, {error, TraceFailureContext}}
        end
    end,
    PayloadIn = MessageIn#message.payload,
    case decode(PayloadIn, PayloadDecoder, Transformation) of
        {ok, InitPayload} ->
            InitAcc = message_to_context(MessageIn, InitPayload, Transformation),
            case emqx_utils:foldl_while(Fun, InitAcc, Operations) of
                #{} = ContextOut ->
                    context_to_message(MessageIn, ContextOut, Transformation);
                {error, TraceFailureContext} ->
                    {FailureAction, TraceFailureContext}
            end;
        {error, TraceFailureContext} ->
            {FailureAction, TraceFailureContext}
    end.

prettify_operation(Operation0) ->
    %% TODO: remove injected bif module
    Operation = maps:update_with(
        value,
        fun(V) -> iolist_to_binary(emqx_variform:decompile(V)) end,
        Operation0
    ),
    maps:update_with(
        key,
        fun(Path) -> iolist_to_binary(lists:join(".", Path)) end,
        Operation
    ).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-spec eval_operation(operation(), transformation(), eval_context()) ->
    {ok, eval_context()} | {error, trace_failure_context()}.
eval_operation(Operation, Transformation, Context) ->
    #{key := K, value := V} = Operation,
    try
        case eval_variform(K, V, Context) of
            {error, Reason} ->
                FailureContext = #trace_failure_context{
                    transformation = Transformation,
                    tag = "transformation_eval_operation_failure",
                    context = #{reason => Reason}
                },
                {error, FailureContext};
            {ok, Rendered} ->
                NewContext = put_value(K, Rendered, Context),
                {ok, NewContext}
        end
    catch
        Class:Error:Stacktrace ->
            FailureContext1 = #trace_failure_context{
                transformation = Transformation,
                tag = "transformation_eval_operation_exception",
                context = #{
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace,
                    operation => prettify_operation(Operation)
                }
            },
            {error, FailureContext1}
    end.

-spec eval_variform([binary(), ...], _, eval_context()) ->
    {ok, rendered_value()} | {error, term()}.
eval_variform(K, V, Context) ->
    Opts =
        case K of
            [<<"payload">> | _] ->
                #{eval_as_string => false};
            _ ->
                #{}
        end,
    case emqx_variform:render(V, Context, Opts) of
        {error, Reason} ->
            {error, Reason};
        {ok, Rendered} ->
            map_result(Rendered, K)
    end.

-spec put_value([binary(), ...], rendered_value(), eval_context()) -> eval_context().
put_value([<<"payload">> | Rest], Rendered, Context0) ->
    Context = maps:update_with(dirty, fun(D) -> D#{payload => true} end, Context0),
    maps:update_with(
        payload,
        fun(P) ->
            case Rest of
                [] ->
                    Rendered;
                _ ->
                    emqx_utils_maps:deep_put(Rest, P, Rendered)
            end
        end,
        Context
    );
put_value([<<"user_property">>, Key], Rendered, Context0) ->
    Context = maps:update_with(dirty, fun(D) -> D#{user_property => true} end, Context0),
    maps:update_with(
        user_property,
        fun(Ps) -> maps:put(Key, Rendered, Ps) end,
        Context
    );
put_value([<<"qos">>], Rendered, Context0) ->
    Context = maps:update_with(dirty, fun(D) -> D#{qos => true} end, Context0),
    Context#{qos := Rendered};
put_value([<<"retain">>], Rendered, Context0) ->
    Context = maps:update_with(dirty, fun(D) -> D#{retain => true} end, Context0),
    Context#{retain := Rendered};
put_value([<<"topic">>], Rendered, Context0) ->
    Context = maps:update_with(dirty, fun(D) -> D#{topic => true} end, Context0),
    Context#{topic := Rendered}.

-spec map_result(binary(), [binary(), ...]) ->
    {ok, 0..2 | boolean() | binary()} | {error, map()}.
map_result(QoSBin, [<<"qos">>]) ->
    case QoSBin of
        <<"0">> -> {ok, 0};
        <<"1">> -> {ok, 1};
        <<"2">> -> {ok, 2};
        _ -> {error, #{reason => bad_qos_value, input => QoSBin}}
    end;
map_result(RetainBin, [<<"retain">>]) ->
    case RetainBin of
        <<"true">> -> {ok, true};
        <<"false">> -> {ok, false};
        _ -> {error, #{reason => bad_retain_value, input => RetainBin}}
    end;
map_result(Rendered, _Key) ->
    {ok, Rendered}.

run_transformations(Transformations, Message = #message{headers = Headers}) ->
    case do_run_transformations(Transformations, Message) of
        #message{} = FinalMessage ->
            emqx_metrics:inc('messages.transformation_succeeded'),
            {ok, FinalMessage};
        drop ->
            emqx_metrics:inc('messages.transformation_failed'),
            {stop, Message#message{headers = Headers#{allow_publish => false}}};
        disconnect ->
            emqx_metrics:inc('messages.transformation_failed'),
            {stop, Message#message{
                headers = Headers#{
                    allow_publish => false,
                    should_disconnect => true
                }
            }}
    end.

do_run_transformations(Transformations, Message) ->
    LastTransformation = #{name := LastTransformationName} = lists:last(Transformations),
    Fun = fun(Transformation, MessageAcc) ->
        #{name := Name} = Transformation,
        emqx_message_transformation_registry:inc_matched(Name),
        case run_transformation(Transformation, MessageAcc) of
            {ok, #message{} = NewAcc} ->
                %% If this is the last transformation, we can't bump its success counter
                %% yet.  We perform a check to see if the final payload is encoded as a
                %% binary after all transformations have run, and it's the last
                %% transformation's responsibility to properly encode it.
                case Name =:= LastTransformationName of
                    true ->
                        ok;
                    false ->
                        emqx_message_transformation_registry:inc_succeeded(Name)
                end,
                {cont, NewAcc};
            {ignore, TraceFailureContext} ->
                trace_failure_from_context(TraceFailureContext),
                emqx_message_transformation_registry:inc_failed(Name),
                run_message_transformation_failed_hook(Message, Transformation),
                {cont, MessageAcc};
            {FailureAction, TraceFailureContext} ->
                trace_failure_from_context(TraceFailureContext),
                trace_failure(Transformation, "transformation_failed", #{
                    transformation => Name,
                    action => FailureAction
                }),
                emqx_message_transformation_registry:inc_failed(Name),
                run_message_transformation_failed_hook(Message, Transformation),
                {halt, FailureAction}
        end
    end,
    case emqx_utils:foldl_while(Fun, Message, Transformations) of
        #message{} = FinalMessage ->
            case is_payload_properly_encoded(FinalMessage) of
                true ->
                    emqx_message_transformation_registry:inc_succeeded(LastTransformationName),
                    FinalMessage;
                false ->
                    %% Take the last validation's failure action, as it's the one
                    %% responsible for getting the right encoding.
                    emqx_message_transformation_registry:inc_failed(LastTransformationName),
                    #{failure_action := FailureAction} = LastTransformation,
                    trace_failure(LastTransformation, "transformation_bad_encoding", #{
                        action => FailureAction,
                        explain => <<"final payload must be encoded as a binary">>
                    }),
                    FailureAction
            end;
        FailureAction ->
            FailureAction
    end.

-spec message_to_context(emqx_types:message(), _Payload, transformation()) -> eval_context().
message_to_context(#message{} = Message, Payload, Transformation) ->
    #{
        payload_decoder := PayloadDecoder,
        payload_encoder := PayloadEncoder
    } = Transformation,
    Dirty =
        case PayloadEncoder =:= PayloadDecoder of
            true -> #{};
            false -> #{payload => true}
        end,
    Flags = emqx_message:get_flags(Message),
    Props = emqx_message:get_header(properties, Message, #{}),
    UserProperties0 = maps:get('User-Property', Props, []),
    UserProperties = maps:from_list(UserProperties0),
    Headers = Message#message.headers,
    Peername =
        case maps:get(peername, Headers, undefined) of
            Peername0 when is_tuple(Peername0) ->
                iolist_to_binary(emqx_utils:ntoa(Peername0));
            _ ->
                undefined
        end,
    Username = maps:get(username, Headers, undefined),
    Timestamp = erlang:system_time(millisecond),
    #{
        dirty => Dirty,

        client_attrs => emqx_message:get_header(client_attrs, Message, #{}),
        clientid => Message#message.from,
        flags => Flags,
        id => emqx_guid:to_hexstr(Message#message.id),
        node => node(),
        payload => Payload,
        peername => Peername,
        pub_props => Props,
        publish_received_at => Message#message.timestamp,
        qos => Message#message.qos,
        retain => emqx_message:get_flag(retain, Message, false),
        timestamp => Timestamp,
        topic => Message#message.topic,
        user_property => UserProperties,
        username => Username
    }.

-spec context_to_message(emqx_types:message(), eval_context(), transformation()) ->
    {ok, emqx_types:message()} | {failure_action(), trace_failure_context()}.
context_to_message(Message, Context, Transformation) ->
    #{
        failure_action := FailureAction,
        payload_encoder := PayloadEncoder
    } = Transformation,
    #{payload := PayloadOut} = Context,
    case encode(PayloadOut, PayloadEncoder, Transformation) of
        {ok, Payload} ->
            {ok, take_from_context(Context#{payload := Payload}, Message)};
        {error, TraceFailureContext} ->
            {FailureAction, TraceFailureContext}
    end.

take_from_context(Context, Message) ->
    maps:fold(
        fun
            (payload, _, Acc) ->
                Acc#message{payload = maps:get(payload, Context)};
            (qos, _, Acc) ->
                Acc#message{qos = maps:get(qos, Context)};
            (topic, _, Acc) ->
                Acc#message{topic = maps:get(topic, Context)};
            (retain, _, Acc) ->
                emqx_message:set_flag(retain, maps:get(retain, Context), Acc);
            (user_property, _, Acc) ->
                Props0 = emqx_message:get_header(properties, Acc, #{}),
                UserProperties0 = maps:to_list(maps:get(user_property, Context)),
                UserProperties = lists:keysort(1, UserProperties0),
                Props = maps:merge(Props0, #{'User-Property' => UserProperties}),
                emqx_message:set_header(properties, Props, Acc)
        end,
        Message,
        maps:get(dirty, Context)
    ).

decode(Payload, #{type := none}, _Transformation) ->
    {ok, Payload};
decode(Payload, #{type := json}, Transformation) when is_binary(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {ok, JSON} ->
            {ok, JSON};
        {error, Reason} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_decode_failed",
                context = #{
                    decoder => json,
                    reason => Reason
                }
            },
            {error, TraceFailureContext}
    end;
decode(Payload, #{type := avro, schema := SerdeName}, Transformation) when is_binary(Payload) ->
    try
        {ok, emqx_schema_registry_serde:decode(SerdeName, Payload)}
    catch
        error:{serde_not_found, _} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_decode_schema_not_found",
                context = #{
                    decoder => avro,
                    schema_name => SerdeName
                }
            },
            {error, TraceFailureContext};
        Class:Error:Stacktrace ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_decode_schema_failure",
                context = #{
                    decoder => avro,
                    schema_name => SerdeName,
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }
            },
            {error, TraceFailureContext}
    end;
decode(
    Payload, #{type := protobuf, schema := SerdeName, message_type := MessageType}, Transformation
) ->
    try
        {ok, emqx_schema_registry_serde:decode(SerdeName, Payload, [MessageType])}
    catch
        error:{serde_not_found, _} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_decode_schema_not_found",
                context = #{
                    decoder => protobuf,
                    schema_name => SerdeName,
                    message_type => MessageType
                }
            },
            {error, TraceFailureContext};
        throw:{schema_decode_error, ExtraContext} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_decode_error",
                context = ExtraContext#{
                    decoder => protobuf,
                    schema_name => SerdeName,
                    message_type => MessageType
                }
            },
            {error, TraceFailureContext};
        Class:Error:Stacktrace ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_decode_schema_failure",
                context = #{
                    decoder => protobuf,
                    schema_name => SerdeName,
                    message_type => MessageType,
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }
            },
            {error, TraceFailureContext}
    end;
decode(NotABinary, #{} = Decoder, Transformation) ->
    DecoderContext0 = maps:with([type, name, message_type], Decoder),
    DecoderContext1 = emqx_utils_maps:rename(name, schema_name, DecoderContext0),
    DecoderContext = emqx_utils_maps:rename(type, decoder, DecoderContext1),
    Context =
        maps:merge(
            DecoderContext,
            #{
                reason => <<"payload must be a binary">>,
                hint => <<"check the transformation(s) before this one for inconsistencies">>,
                bad_payload => NotABinary
            }
        ),
    TraceFailureContext = #trace_failure_context{
        transformation = Transformation,
        tag = "payload_decode_failed",
        context = Context
    },
    {error, TraceFailureContext}.

encode(Payload, #{type := none}, _Transformation) ->
    {ok, Payload};
encode(Payload, #{type := json}, Transformation) ->
    case emqx_utils_json:safe_encode(Payload) of
        {ok, Bin} ->
            {ok, Bin};
        {error, Reason} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_encode_failed",
                context = #{
                    encoder => json,
                    reason => Reason
                }
            },
            {error, TraceFailureContext}
    end;
encode(Payload, #{type := avro, schema := SerdeName}, Transformation) ->
    try
        {ok, emqx_schema_registry_serde:encode(SerdeName, Payload)}
    catch
        error:{serde_not_found, _} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_encode_schema_not_found",
                context = #{
                    encoder => avro,
                    schema_name => SerdeName
                }
            },
            {error, TraceFailureContext};
        Class:Error:Stacktrace ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_encode_schema_failure",
                context = #{
                    encoder => avro,
                    schema_name => SerdeName,
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }
            },
            {error, TraceFailureContext}
    end;
encode(
    Payload, #{type := protobuf, schema := SerdeName, message_type := MessageType}, Transformation
) ->
    try
        {ok, emqx_schema_registry_serde:encode(SerdeName, Payload, [MessageType])}
    catch
        error:{serde_not_found, _} ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_encode_schema_failure",
                context = #{
                    encoder => protobuf,
                    schema_name => SerdeName,
                    message_type => MessageType
                }
            },
            {error, TraceFailureContext};
        Class:Error:Stacktrace ->
            TraceFailureContext = #trace_failure_context{
                transformation = Transformation,
                tag = "payload_encode_schema_failure",
                context = #{
                    encoder => protobuf,
                    schema_name => SerdeName,
                    message_type => MessageType,
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }
            },
            {error, TraceFailureContext}
    end.

trace_failure_from_context(
    #trace_failure_context{
        transformation = Transformation,
        tag = Tag,
        context = Context
    }
) ->
    trace_failure(Transformation, Tag, Context).

%% Internal export for HTTP API.
trace_failure_context_to_map(
    #trace_failure_context{
        tag = Tag,
        context = Context
    }
) ->
    Context#{msg => list_to_binary(Tag)}.

trace_failure(#{log_failure := #{level := none}} = Transformation, _Msg, _Meta) ->
    #{
        name := _Name,
        failure_action := _Action
    } = Transformation,
    ?tp(message_transformation_failed, _Meta#{log_level => none, name => _Name, message => _Msg}),
    ok;
trace_failure(#{log_failure := #{level := Level}} = Transformation, Msg, Meta0) ->
    #{
        name := Name,
        failure_action := _Action
    } = Transformation,
    Meta = maps:merge(#{name => Name}, Meta0),
    ?tp(message_transformation_failed, Meta#{
        log_level => Level, name => Name, action => _Action, message => Msg
    }),
    ?TRACE(Level, ?TRACE_TAG, Msg, Meta).

run_message_transformation_failed_hook(Message, Transformation) ->
    #{name := Name} = Transformation,
    TransformationContext = #{name => Name},
    emqx_hooks:run('message.transformation_failed', [Message, TransformationContext]).

is_payload_properly_encoded(#message{payload = Payload}) ->
    try iolist_size(Payload) of
        _ ->
            true
    catch
        error:badarg ->
            false
    end.
