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
    add_handler/0,
    remove_handler/0,

    load/0,
    unload/0,

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

%% `emqx_config_handler' API
-export([pre_config_update/3, post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).

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

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec add_handler() -> ok.
add_handler() ->
    ok = emqx_config_handler:add_handler([?CONF_ROOT], ?MODULE),
    ok = emqx_config_handler:add_handler(?TRANSFORMATIONS_CONF_PATH, ?MODULE),
    ok.

-spec remove_handler() -> ok.
remove_handler() ->
    ok = emqx_config_handler:remove_handler(?TRANSFORMATIONS_CONF_PATH),
    ok = emqx_config_handler:remove_handler([?CONF_ROOT]),
    ok.

load() ->
    Transformations = emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []),
    lists:foreach(
        fun({Pos, Transformation}) ->
            ok = emqx_message_transformation_registry:insert(Pos, Transformation)
        end,
        lists:enumerate(Transformations)
    ).

unload() ->
    Transformations = emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []),
    lists:foreach(
        fun(Transformation) ->
            ok = emqx_message_transformation_registry:delete(Transformation)
        end,
        Transformations
    ).

-spec list() -> [transformation()].
list() ->
    emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []).

-spec reorder([transformation_name()]) ->
    {ok, _} | {error, _}.
reorder(Order) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {reorder, Order},
        #{override_to => cluster}
    ).

-spec lookup(transformation_name()) -> {ok, transformation()} | {error, not_found}.
lookup(Name) ->
    Transformations = emqx:get_config(?TRANSFORMATIONS_CONF_PATH, []),
    do_lookup(Name, Transformations).

-spec insert(transformation()) ->
    {ok, _} | {error, _}.
insert(Transformation) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {append, Transformation},
        #{override_to => cluster}
    ).

-spec update(transformation()) ->
    {ok, _} | {error, _}.
update(Transformation) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {update, Transformation},
        #{override_to => cluster}
    ).

-spec delete(transformation_name()) ->
    {ok, _} | {error, _}.
delete(Name) ->
    emqx_conf:update(
        ?TRANSFORMATIONS_CONF_PATH,
        {delete, Name},
        #{override_to => cluster}
    ).

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
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

pre_config_update(?TRANSFORMATIONS_CONF_PATH, {append, Transformation}, OldTransformations) ->
    Transformations = OldTransformations ++ [Transformation],
    {ok, Transformations};
pre_config_update(?TRANSFORMATIONS_CONF_PATH, {update, Transformation}, OldTransformations) ->
    replace(OldTransformations, Transformation);
pre_config_update(?TRANSFORMATIONS_CONF_PATH, {delete, Transformation}, OldTransformations) ->
    delete(OldTransformations, Transformation);
pre_config_update(?TRANSFORMATIONS_CONF_PATH, {reorder, Order}, OldTransformations) ->
    reorder(OldTransformations, Order);
pre_config_update([?CONF_ROOT], {merge, NewConfig}, OldConfig) ->
    #{resulting_config := Config} = prepare_config_merge(NewConfig, OldConfig),
    {ok, Config};
pre_config_update([?CONF_ROOT], {replace, NewConfig}, _OldConfig) ->
    {ok, NewConfig}.

post_config_update(
    ?TRANSFORMATIONS_CONF_PATH, {append, #{<<"name">> := Name}}, New, _Old, _AppEnvs
) ->
    {Pos, Transformation} = fetch_with_index(New, Name),
    ok = emqx_message_transformation_registry:insert(Pos, Transformation),
    ok;
post_config_update(?TRANSFORMATIONS_CONF_PATH, {update, #{<<"name">> := Name}}, New, Old, _AppEnvs) ->
    {_Pos, OldTransformation} = fetch_with_index(Old, Name),
    {Pos, NewTransformation} = fetch_with_index(New, Name),
    ok = emqx_message_transformation_registry:update(OldTransformation, Pos, NewTransformation),
    ok;
post_config_update(?TRANSFORMATIONS_CONF_PATH, {delete, Name}, _New, Old, _AppEnvs) ->
    {_Pos, Transformation} = fetch_with_index(Old, Name),
    ok = emqx_message_transformation_registry:delete(Transformation),
    ok;
post_config_update(?TRANSFORMATIONS_CONF_PATH, {reorder, _Order}, New, _Old, _AppEnvs) ->
    ok = emqx_message_transformation_registry:reindex_positions(New),
    ok;
post_config_update([?CONF_ROOT], {merge, _}, ResultingConfig, Old, _AppEnvs) ->
    #{transformations := ResultingTransformations} = ResultingConfig,
    #{transformations := OldTransformations} = Old,
    #{added := NewTransformations0} =
        emqx_utils:diff_lists(
            ResultingTransformations,
            OldTransformations,
            fun(#{name := N}) -> N end
        ),
    NewTransformations =
        lists:map(
            fun(#{name := Name}) ->
                {Pos, Transformation} = fetch_with_index(ResultingTransformations, Name),
                ok = emqx_message_transformation_registry:insert(Pos, Transformation),
                #{name => Name, pos => Pos}
            end,
            NewTransformations0
        ),
    {ok, #{new_transformations => NewTransformations}};
post_config_update([?CONF_ROOT], {replace, Input}, ResultingConfig, Old, _AppEnvs) ->
    #{
        new_transformations := NewTransformations,
        changed_transformations := ChangedTransformations0,
        deleted_transformations := DeletedTransformations
    } = prepare_config_replace(Input, Old),
    #{transformations := ResultingTransformations} = ResultingConfig,
    #{transformations := OldTransformations} = Old,
    lists:foreach(
        fun(Name) ->
            {_Pos, Transformation} = fetch_with_index(OldTransformations, Name),
            ok = emqx_message_transformation_registry:delete(Transformation)
        end,
        DeletedTransformations
    ),
    lists:foreach(
        fun(Name) ->
            {Pos, Transformation} = fetch_with_index(ResultingTransformations, Name),
            ok = emqx_message_transformation_registry:insert(Pos, Transformation)
        end,
        NewTransformations
    ),
    ChangedTransformations =
        lists:map(
            fun(Name) ->
                {_Pos, OldTransformation} = fetch_with_index(OldTransformations, Name),
                {Pos, NewTransformation} = fetch_with_index(ResultingTransformations, Name),
                ok = emqx_message_transformation_registry:update(
                    OldTransformation, Pos, NewTransformation
                ),
                #{name => Name, pos => Pos}
            end,
            ChangedTransformations0
        ),
    ok = emqx_message_transformation_registry:reindex_positions(ResultingTransformations),
    {ok, #{changed_transformations => ChangedTransformations}}.

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(#{?CONF_ROOT_BIN := RawConf0}) ->
    Result = emqx_conf:update(
        [?CONF_ROOT],
        {merge, RawConf0},
        #{override_to => cluster, rawconf_with_defaults => true}
    ),
    case Result of
        {error, Reason} ->
            {error, #{root_key => ?CONF_ROOT, reason => Reason}};
        {ok, _} ->
            Keys0 = maps:keys(RawConf0),
            ChangedPaths = Keys0 -- [<<"transformations">>],
            {ok, #{root_key => ?CONF_ROOT, changed => ChangedPaths}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_ROOT, changed => []}}.

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

replace(OldTransformations, Transformation = #{<<"name">> := Name}) ->
    {Found, RevNewTransformations} =
        lists:foldl(
            fun
                (#{<<"name">> := NameIn}, {_FoundIn, Acc}) when NameIn =:= Name ->
                    {true, [Transformation | Acc]};
                (Val, {FoundIn, Acc}) ->
                    {FoundIn, [Val | Acc]}
            end,
            {false, []},
            OldTransformations
        ),
    case Found of
        true ->
            {ok, lists:reverse(RevNewTransformations)};
        false ->
            {error, not_found}
    end.

delete(OldTransformations, Name) ->
    {Found, RevNewTransformations} =
        lists:foldl(
            fun
                (#{<<"name">> := NameIn}, {_FoundIn, Acc}) when NameIn =:= Name ->
                    {true, Acc};
                (Val, {FoundIn, Acc}) ->
                    {FoundIn, [Val | Acc]}
            end,
            {false, []},
            OldTransformations
        ),
    case Found of
        true ->
            {ok, lists:reverse(RevNewTransformations)};
        false ->
            {error, not_found}
    end.

reorder(Transformations, Order) ->
    Context = #{
        not_found => sets:new([{version, 2}]),
        duplicated => sets:new([{version, 2}]),
        res => [],
        seen => sets:new([{version, 2}])
    },
    reorder(Transformations, Order, Context).

reorder(NotReordered, _Order = [], #{not_found := NotFound0, duplicated := Duplicated0, res := Res}) ->
    NotFound = sets:to_list(NotFound0),
    Duplicated = sets:to_list(Duplicated0),
    case {NotReordered, NotFound, Duplicated} of
        {[], [], []} ->
            {ok, lists:reverse(Res)};
        {_, _, _} ->
            Error = #{
                not_found => NotFound,
                duplicated => Duplicated,
                not_reordered => [N || #{<<"name">> := N} <- NotReordered]
            },
            {error, Error}
    end;
reorder(RemainingTransformations, [Name | Rest], Context0 = #{seen := Seen0}) ->
    case sets:is_element(Name, Seen0) of
        true ->
            Context = maps:update_with(
                duplicated, fun(S) -> sets:add_element(Name, S) end, Context0
            ),
            reorder(RemainingTransformations, Rest, Context);
        false ->
            case safe_take(Name, RemainingTransformations) of
                error ->
                    Context = maps:update_with(
                        not_found, fun(S) -> sets:add_element(Name, S) end, Context0
                    ),
                    reorder(RemainingTransformations, Rest, Context);
                {ok, {Transformation, Front, Rear}} ->
                    Context1 = maps:update_with(
                        seen, fun(S) -> sets:add_element(Name, S) end, Context0
                    ),
                    Context = maps:update_with(res, fun(Vs) -> [Transformation | Vs] end, Context1),
                    reorder(Front ++ Rear, Rest, Context)
            end
    end.

fetch_with_index([{Pos, #{name := Name} = Transformation} | _Rest], Name) ->
    {Pos, Transformation};
fetch_with_index([{_, _} | Rest], Name) ->
    fetch_with_index(Rest, Name);
fetch_with_index(Transformations, Name) ->
    fetch_with_index(lists:enumerate(Transformations), Name).

safe_take(Name, Transformations) ->
    case lists:splitwith(fun(#{<<"name">> := N}) -> N =/= Name end, Transformations) of
        {_Front, []} ->
            error;
        {Front, [Found | Rear]} ->
            {ok, {Found, Front, Rear}}
    end.

do_lookup(_Name, _Transformations = []) ->
    {error, not_found};
do_lookup(Name, [#{name := Name} = Transformation | _Rest]) ->
    {ok, Transformation};
do_lookup(Name, [_ | Rest]) ->
    do_lookup(Name, Rest).

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

%% "Merging" in the context of the transformation array means:
%%   * Existing transformations (identified by `name') are left untouched.
%%   * No transformations are removed.
%%   * New transformations are appended to the existing list.
%%   * Existing transformations are not reordered.
prepare_config_merge(NewConfig0, OldConfig) ->
    {ImportedRawTransformations, NewConfigNoTransformations} =
        case maps:take(<<"transformations">>, NewConfig0) of
            error ->
                {[], NewConfig0};
            {V, R} ->
                {V, R}
        end,
    OldRawTransformations = maps:get(<<"transformations">>, OldConfig, []),
    #{added := NewRawTransformations} = emqx_utils:diff_lists(
        ImportedRawTransformations,
        OldRawTransformations,
        fun(#{<<"name">> := N}) -> N end
    ),
    Config0 = emqx_utils_maps:deep_merge(OldConfig, NewConfigNoTransformations),
    Config = maps:update_with(
        <<"transformations">>,
        fun(OldVs) -> OldVs ++ NewRawTransformations end,
        NewRawTransformations,
        Config0
    ),
    #{
        new_transformations => NewRawTransformations,
        resulting_config => Config
    }.

prepare_config_replace(NewConfig, OldConfig) ->
    ImportedRawTransformations = maps:get(<<"transformations">>, NewConfig, []),
    OldTransformations = maps:get(transformations, OldConfig, []),
    %% Since, at this point, we have an input raw config but a parsed old config, we
    %% project both to the to have only their names, and consider common names as changed.
    #{
        added := NewTransformations,
        removed := DeletedTransformations,
        changed := ChangedTransformations0,
        identical := ChangedTransformations1
    } = emqx_utils:diff_lists(
        lists:map(fun(#{<<"name">> := N}) -> N end, ImportedRawTransformations),
        lists:map(fun(#{name := N}) -> N end, OldTransformations),
        fun(N) -> N end
    ),
    #{
        new_transformations => NewTransformations,
        changed_transformations => ChangedTransformations0 ++ ChangedTransformations1,
        deleted_transformations => DeletedTransformations
    }.
