%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation).

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
-export([parse_sql_check/1]).

%% Internal functions; exported for tests
-export([
    evaluate_sql_check/3
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(TRACE_TAG, "SCHEMA_VALIDATION").

-type validation_name() :: binary().
-type raw_validation() :: #{binary() => _}.
-type validation() :: #{
    name := validation_name(),
    strategy := all_pass | any_pass,
    failure_action := drop | disconnect | ignore,
    log_failure := #{level := error | warning | notice | info | debug | none}
}.

-export_type([
    validation/0,
    validation_name/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec list() -> [validation()].
list() ->
    emqx_schema_validation_config:list().

-spec reorder([validation_name()]) ->
    {ok, _} | {error, _}.
reorder(Order) ->
    emqx_schema_validation_config:reorder(Order).

-spec lookup(validation_name()) -> {ok, validation()} | {error, not_found}.
lookup(Name) ->
    emqx_schema_validation_config:lookup(Name).

-spec insert(raw_validation()) ->
    {ok, _} | {error, _}.
insert(Validation) ->
    emqx_schema_validation_config:insert(Validation).

-spec update(raw_validation()) ->
    {ok, _} | {error, _}.
update(Validation) ->
    emqx_schema_validation_config:update(Validation).

-spec delete(validation_name()) ->
    {ok, _} | {error, _}.
delete(Name) ->
    emqx_schema_validation_config:delete(Name).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_SCHEMA_VALIDATION).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}).

-spec on_message_publish(emqx_types:message()) ->
    {ok, emqx_types:message()} | {stop, emqx_types:message()}.
on_message_publish(Message = #message{topic = Topic, headers = Headers}) ->
    case emqx_schema_validation_registry:matching_validations(Topic) of
        [] ->
            ok;
        Validations ->
            case run_validations(Validations, Message) of
                ok ->
                    emqx_metrics:inc('messages.validation_succeeded'),
                    {ok, Message};
                drop ->
                    emqx_metrics:inc('messages.validation_failed'),
                    {stop, Message#message{headers = Headers#{allow_publish => false}}};
                disconnect ->
                    emqx_metrics:inc('messages.validation_failed'),
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

parse_sql_check(SQL) ->
    case emqx_rule_sqlparser:parse(SQL, #{with_from => false}) of
        {ok, Select} ->
            case emqx_rule_sqlparser:select_is_foreach(Select) of
                true ->
                    {error, foreach_not_allowed};
                false ->
                    Check = #{
                        type => sql,
                        fields => emqx_rule_sqlparser:select_fields(Select),
                        conditions => emqx_rule_sqlparser:select_where(Select)
                    },
                    {ok, Check}
            end;
        Error = {error, _} ->
            Error
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

evaluate_sql_check(Check, Validation, Message) ->
    #{
        fields := Fields,
        conditions := Conditions
    } = Check,
    #{name := Name} = Validation,
    {Data, _} = emqx_rule_events:eventmsg_publish(Message),
    try emqx_rule_runtime:evaluate_select(Fields, Data, Conditions) of
        {ok, _} ->
            true;
        false ->
            false
    catch
        throw:Reason ->
            trace_failure(Validation, "validation_sql_check_throw", #{
                validation => Name,
                reason => Reason
            }),
            false;
        Class:Error:Stacktrace ->
            trace_failure(Validation, "validation_sql_check_failure", #{
                validation => Name,
                kind => Class,
                reason => Error,
                stacktrace => Stacktrace
            }),
            false
    end.

evaluate_schema_check(Check, Validation, #message{payload = Data}) ->
    #{schema := SerdeName} = Check,
    #{name := Name} = Validation,
    ExtraArgs =
        case Check of
            #{type := protobuf, message_type := MessageType} ->
                [MessageType];
            _ ->
                []
        end,
    try
        emqx_schema_registry_serde:schema_check(SerdeName, Data, ExtraArgs)
    catch
        error:{serde_not_found, _} ->
            trace_failure(Validation, "validation_schema_check_schema_not_found", #{
                validation => Name,
                schema_name => SerdeName
            }),
            false;
        Class:Error:Stacktrace ->
            trace_failure(Validation, "validation_schema_check_failure", #{
                validation => Name,
                schema_name => SerdeName,
                kind => Class,
                reason => Error,
                stacktrace => Stacktrace
            }),
            false
    end.

run_validations(Validations, Message) ->
    try
        emqx_rule_runtime:clear_rule_payload(),
        Fun = fun(Validation, Acc) ->
            #{name := Name} = Validation,
            emqx_schema_validation_registry:inc_matched(Name),
            case run_validation(Validation, Message) of
                ok ->
                    emqx_schema_validation_registry:inc_succeeded(Name),
                    {cont, Acc};
                ignore ->
                    trace_failure(Validation, "validation_failed", #{
                        validation => Name,
                        action => ignore
                    }),
                    emqx_schema_validation_registry:inc_failed(Name),
                    run_schema_validation_failed_hook(Message, Validation),
                    {cont, Acc};
                FailureAction ->
                    trace_failure(Validation, "validation_failed", #{
                        validation => Name,
                        action => FailureAction
                    }),
                    emqx_schema_validation_registry:inc_failed(Name),
                    run_schema_validation_failed_hook(Message, Validation),
                    {halt, FailureAction}
            end
        end,
        emqx_utils:foldl_while(Fun, _Passed = ok, Validations)
    after
        emqx_rule_runtime:clear_rule_payload()
    end.

run_validation(#{strategy := all_pass} = Validation, Message) ->
    #{
        checks := Checks,
        failure_action := FailureAction
    } = Validation,
    Fun = fun(Check, Acc) ->
        case run_check(Check, Validation, Message) of
            true -> {cont, Acc};
            false -> {halt, FailureAction}
        end
    end,
    emqx_utils:foldl_while(Fun, _Passed = ok, Checks);
run_validation(#{strategy := any_pass} = Validation, Message) ->
    #{
        checks := Checks,
        failure_action := FailureAction
    } = Validation,
    case lists:any(fun(C) -> run_check(C, Validation, Message) end, Checks) of
        true ->
            ok;
        false ->
            FailureAction
    end.

run_check(#{type := sql} = Check, Validation, Message) ->
    evaluate_sql_check(Check, Validation, Message);
run_check(Check, Validation, Message) ->
    evaluate_schema_check(Check, Validation, Message).

trace_failure(#{log_failure := #{level := none}} = Validation, _Msg, _Meta) ->
    #{
        name := _Name,
        failure_action := _Action
    } = Validation,
    ?tp(schema_validation_failed, #{log_level => none, name => _Name, action => _Action}),
    ok;
trace_failure(#{log_failure := #{level := Level}} = Validation, Msg, Meta) ->
    #{
        name := _Name,
        failure_action := _Action
    } = Validation,
    ?tp(schema_validation_failed, #{log_level => Level, name => _Name, action => _Action}),
    ?TRACE(Level, ?TRACE_TAG, Msg, Meta).

run_schema_validation_failed_hook(Message, Validation) ->
    #{name := Name} = Validation,
    ValidationContext = #{name => Name},
    emqx_hooks:run('schema.validation_failed', [Message, ValidationContext]).
