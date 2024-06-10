%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation).

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

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(TRACE_TAG, "MESSAGE_TRANSFORMATION").
-define(CONF_ROOT, message_transformation).
-define(CONF_ROOT_BIN, <<"message_transformation">>).
-define(TRANSFORMATIONS_CONF_PATH, [?CONF_ROOT, transformations]).

-type transformation_name() :: binary().
%% TODO: make more specific typespec
-type transformation() :: #{atom() => term()}.
%% TODO: make more specific typespec
-type variform() :: any().
-type operation() :: #{key := [binary(), ...], value := variform()}.
-type qos() :: 0..2.
-type rendered_value() :: qos() | boolean() | binary().

-type eval_context() :: #{
    payload := _,
    qos := _,
    retain := _,
    topic := _,
    user_property := _,
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
    transformation_name/0
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
on_message_publish(Message = #message{topic = Topic, headers = Headers}) ->
    case emqx_message_transformation_registry:matching_transformations(Topic) of
        [] ->
            ok;
        Transformations ->
            case run_transformations(Transformations, Message) of
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
            end
    end.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-spec eval_operation(operation(), transformation(), eval_context()) -> {ok, eval_context()} | error.
eval_operation(Operation, Transformation, Context) ->
    #{key := K, value := V} = Operation,
    case eval_variform(K, V, Context) of
        {error, Reason} ->
            trace_failure(Transformation, "transformation_eval_operation_failure", #{
                reason => Reason
            }),
            error;
        {ok, Rendered} ->
            NewContext = put_value(K, Rendered, Context),
            {ok, NewContext}
    end.

-spec eval_variform([binary(), ...], _, eval_context()) ->
    {ok, rendered_value()} | {error, term()}.
eval_variform(K, V, Context) ->
    case emqx_variform:render(V, Context) of
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
        fun(Ps) -> lists:keystore(Key, 1, Ps, {Key, Rendered}) end,
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

run_transformations(Transformations, Message) ->
    Fun = fun(Transformation, MessageAcc) ->
        #{name := Name} = Transformation,
        emqx_message_transformation_registry:inc_matched(Name),
        case run_transformation(Transformation, MessageAcc) of
            #message{} = NewAcc ->
                emqx_message_transformation_registry:inc_succeeded(Name),
                {cont, NewAcc};
            ignore ->
                emqx_message_transformation_registry:inc_failed(Name),
                run_message_transformation_failed_hook(Message, Transformation),
                {cont, MessageAcc};
            FailureAction ->
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
                    FinalMessage;
                false ->
                    %% Take the last validation's failure action, as it's the one
                    %% responsible for getting the right encoding.
                    LastTransformation = lists:last(Transformations),
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

run_transformation(Transformation, MessageIn) ->
    #{
        operations := Operations,
        failure_action := FailureAction,
        payload_decoder := PayloadDecoder
    } = Transformation,
    Fun = fun(Operation, Acc) ->
        case eval_operation(Operation, Transformation, Acc) of
            {ok, NewAcc} -> {cont, NewAcc};
            error -> {halt, FailureAction}
        end
    end,
    PayloadIn = MessageIn#message.payload,
    case decode(PayloadIn, PayloadDecoder, Transformation) of
        {ok, InitPayload} ->
            InitAcc = message_to_context(MessageIn, InitPayload, Transformation),
            case emqx_utils:foldl_while(Fun, InitAcc, Operations) of
                #{} = ContextOut ->
                    context_to_message(MessageIn, ContextOut, Transformation);
                _ ->
                    FailureAction
            end;
        error ->
            %% Error already logged
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
    #{
        dirty => Dirty,
        payload => Payload,
        qos => Message#message.qos,
        retain => emqx_message:get_flag(retain, Message, false),
        topic => Message#message.topic,
        user_property => maps:get(
            'User-Property', emqx_message:get_header(properties, Message, #{}), []
        )
    }.

-spec context_to_message(emqx_types:message(), eval_context(), transformation()) ->
    {ok, emqx_types:message()} | _TODO.
context_to_message(Message, Context, Transformation) ->
    #{
        failure_action := FailureAction,
        payload_encoder := PayloadEncoder
    } = Transformation,
    #{payload := PayloadOut} = Context,
    case encode(PayloadOut, PayloadEncoder, Transformation) of
        {ok, Payload} ->
            take_from_context(Context#{payload := Payload}, Message);
        error ->
            FailureAction
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
                Props = maps:merge(Props0, #{'User-Property' => maps:get(user_property, Context)}),
                emqx_message:set_header(properties, Props, Acc)
        end,
        Message,
        maps:get(dirty, Context)
    ).

decode(Payload, #{type := none}, _Transformation) ->
    {ok, Payload};
decode(Payload, #{type := json}, Transformation) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {ok, JSON} ->
            {ok, JSON};
        {error, Reason} ->
            trace_failure(Transformation, "payload_decode_failed", #{
                decoder => json,
                reason => Reason
            }),
            error
    end;
decode(Payload, #{type := avro, schema := SerdeName}, Transformation) ->
    try
        {ok, emqx_schema_registry_serde:decode(SerdeName, Payload)}
    catch
        error:{serde_not_found, _} ->
            trace_failure(Transformation, "payload_decode_schema_not_found", #{
                decoder => avro,
                schema_name => SerdeName
            }),
            error;
        Class:Error:Stacktrace ->
            trace_failure(Transformation, "payload_decode_schema_failure", #{
                decoder => avro,
                schema_name => SerdeName,
                kind => Class,
                reason => Error,
                stacktrace => Stacktrace
            }),
            error
    end;
decode(
    Payload, #{type := protobuf, schema := SerdeName, message_type := MessageType}, Transformation
) ->
    try
        {ok, emqx_schema_registry_serde:decode(SerdeName, Payload, [MessageType])}
    catch
        error:{serde_not_found, _} ->
            trace_failure(Transformation, "payload_decode_schema_not_found", #{
                decoder => protobuf,
                schema_name => SerdeName,
                message_type => MessageType
            }),
            error;
        Class:Error:Stacktrace ->
            trace_failure(Transformation, "payload_decode_schema_failure", #{
                decoder => protobuf,
                schema_name => SerdeName,
                message_type => MessageType,
                kind => Class,
                reason => Error,
                stacktrace => Stacktrace
            }),
            error
    end.

encode(Payload, #{type := none}, _Transformation) ->
    {ok, Payload};
encode(Payload, #{type := json}, Transformation) ->
    case emqx_utils_json:safe_encode(Payload) of
        {ok, Bin} ->
            {ok, Bin};
        {error, Reason} ->
            trace_failure(Transformation, "payload_encode_failed", #{
                encoder => json,
                reason => Reason
            }),
            error
    end;
encode(Payload, #{type := avro, schema := SerdeName}, Transformation) ->
    try
        {ok, emqx_schema_registry_serde:encode(SerdeName, Payload)}
    catch
        error:{serde_not_found, _} ->
            trace_failure(Transformation, "payload_encode_schema_not_found", #{
                encoder => avro,
                schema_name => SerdeName
            }),
            error;
        Class:Error:Stacktrace ->
            trace_failure(Transformation, "payload_encode_schema_failure", #{
                encoder => avro,
                schema_name => SerdeName,
                kind => Class,
                reason => Error,
                stacktrace => Stacktrace
            }),
            error
    end;
encode(
    Payload, #{type := protobuf, schema := SerdeName, message_type := MessageType}, Transformation
) ->
    try
        {ok, emqx_schema_registry_serde:encode(SerdeName, Payload, [MessageType])}
    catch
        error:{serde_not_found, _} ->
            trace_failure(Transformation, "payload_encode_schema_not_found", #{
                encoder => protobuf,
                schema_name => SerdeName,
                message_type => MessageType
            }),
            error;
        Class:Error:Stacktrace ->
            trace_failure(Transformation, "payload_encode_schema_failure", #{
                encoder => protobuf,
                schema_name => SerdeName,
                message_type => MessageType,
                kind => Class,
                reason => Error,
                stacktrace => Stacktrace
            }),
            error
    end.

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
