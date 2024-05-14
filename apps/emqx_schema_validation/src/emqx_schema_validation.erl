%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation).

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
-define(CONF_ROOT, schema_validation).
-define(CONF_ROOT_BIN, <<"schema_validation">>).
-define(VALIDATIONS_CONF_PATH, [?CONF_ROOT, validations]).

-type validation_name() :: binary().
-type validation() :: _TODO.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec add_handler() -> ok.
add_handler() ->
    ok = emqx_config_handler:add_handler(?VALIDATIONS_CONF_PATH, ?MODULE),
    ok.

-spec remove_handler() -> ok.
remove_handler() ->
    ok = emqx_config_handler:remove_handler(?VALIDATIONS_CONF_PATH),
    ok.

load() ->
    Validations = emqx:get_config(?VALIDATIONS_CONF_PATH, []),
    lists:foreach(
        fun({Pos, Validation}) ->
            ok = emqx_schema_validation_registry:insert(Pos, Validation)
        end,
        lists:enumerate(Validations)
    ).

unload() ->
    Validations = emqx:get_config(?VALIDATIONS_CONF_PATH, []),
    lists:foreach(
        fun(Validation) ->
            ok = emqx_schema_validation_registry:delete(Validation)
        end,
        Validations
    ).

-spec list() -> [validation()].
list() ->
    emqx:get_config(?VALIDATIONS_CONF_PATH, []).

-spec reorder([validation_name()]) ->
    {ok, _} | {error, _}.
reorder(Order) ->
    emqx_conf:update(
        ?VALIDATIONS_CONF_PATH,
        {reorder, Order},
        #{override_to => cluster}
    ).

-spec lookup(validation_name()) -> {ok, validation()} | {error, not_found}.
lookup(Name) ->
    Validations = emqx:get_config(?VALIDATIONS_CONF_PATH, []),
    do_lookup(Name, Validations).

-spec insert(validation()) ->
    {ok, _} | {error, _}.
insert(Validation) ->
    emqx_conf:update(
        ?VALIDATIONS_CONF_PATH,
        {append, Validation},
        #{override_to => cluster}
    ).

-spec update(validation()) ->
    {ok, _} | {error, _}.
update(Validation) ->
    emqx_conf:update(
        ?VALIDATIONS_CONF_PATH,
        {update, Validation},
        #{override_to => cluster}
    ).

-spec delete(validation_name()) ->
    {ok, _} | {error, _}.
delete(Name) ->
    emqx_conf:update(
        ?VALIDATIONS_CONF_PATH,
        {delete, Name},
        #{override_to => cluster}
    ).

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_MSG_VALIDATION).

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
%% `emqx_config_handler' API
%%------------------------------------------------------------------------------

pre_config_update(?VALIDATIONS_CONF_PATH, {append, Validation}, OldValidations) ->
    Validations = OldValidations ++ [Validation],
    {ok, Validations};
pre_config_update(?VALIDATIONS_CONF_PATH, {update, Validation}, OldValidations) ->
    replace(OldValidations, Validation);
pre_config_update(?VALIDATIONS_CONF_PATH, {delete, Validation}, OldValidations) ->
    delete(OldValidations, Validation);
pre_config_update(?VALIDATIONS_CONF_PATH, {reorder, Order}, OldValidations) ->
    reorder(OldValidations, Order).

post_config_update(?VALIDATIONS_CONF_PATH, {append, #{<<"name">> := Name}}, New, _Old, _AppEnvs) ->
    {Pos, Validation} = fetch_with_index(New, Name),
    ok = emqx_schema_validation_registry:insert(Pos, Validation),
    ok;
post_config_update(?VALIDATIONS_CONF_PATH, {update, #{<<"name">> := Name}}, New, Old, _AppEnvs) ->
    {_Pos, OldValidation} = fetch_with_index(Old, Name),
    {Pos, NewValidation} = fetch_with_index(New, Name),
    ok = emqx_schema_validation_registry:update(OldValidation, Pos, NewValidation),
    ok;
post_config_update(?VALIDATIONS_CONF_PATH, {delete, Name}, _New, Old, _AppEnvs) ->
    {_Pos, Validation} = fetch_with_index(Old, Name),
    ok = emqx_schema_validation_registry:delete(Validation),
    ok;
post_config_update(?VALIDATIONS_CONF_PATH, {reorder, _Order}, New, _Old, _AppEnvs) ->
    ok = emqx_schema_validation_registry:reindex_positions(New),
    ok.

%%------------------------------------------------------------------------------
%% `emqx_config_backup' API
%%------------------------------------------------------------------------------

import_config(#{?CONF_ROOT_BIN := RawConf0}) ->
    OldRawValidations = emqx_config:get_raw([?CONF_ROOT_BIN, <<"validations">>], []),
    {ImportedRawValidations, RawConf1} =
        case maps:take(<<"validations">>, RawConf0) of
            error ->
                {[], RawConf0};
            {V, R} ->
                {V, R}
        end,
    %% If there's a matching validation, we don't overwrite it.  We don't remove any
    %% validations, either.
    #{added := NewRawValidations} = emqx_utils:diff_lists(
        ImportedRawValidations,
        OldRawValidations,
        fun(#{<<"name">> := N}) -> N end
    ),
    OtherConfs = maps:to_list(RawConf1),
    Result = emqx_utils:foldl_while(
        fun
            ({validation, RawValidation}, Acc) ->
                case insert(RawValidation) of
                    {ok, _} ->
                        #{<<"name">> := N} = RawValidation,
                        ChangedPath = [?CONF_ROOT, validations, '?', N],
                        {cont, [ChangedPath | Acc]};
                    {error, _} = Error ->
                        {halt, Error}
                end;
            ({Key, RawConf}, Acc) ->
                case emqx_conf:update([?CONF_ROOT_BIN, Key], RawConf, #{override_to => cluster}) of
                    {ok, _} ->
                        ChangedPath = [?CONF_ROOT, Key],
                        {conf, [ChangedPath | Acc]};
                    {error, _} = Error ->
                        {halt, Error}
                end
        end,
        [],
        OtherConfs ++
            [{validation, NewValidation} || NewValidation <- NewRawValidations]
    ),
    case Result of
        {error, Reason} ->
            {error, #{root_key => ?CONF_ROOT, reason => Reason}};
        ChangedPaths ->
            {ok, #{root_key => ?CONF_ROOT, changed => ChangedPaths}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_ROOT, changed => []}}.

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

replace(OldValidations, Validation = #{<<"name">> := Name}) ->
    {Found, RevNewValidations} =
        lists:foldl(
            fun
                (#{<<"name">> := NameIn}, {_FoundIn, Acc}) when NameIn =:= Name ->
                    {true, [Validation | Acc]};
                (Val, {FoundIn, Acc}) ->
                    {FoundIn, [Val | Acc]}
            end,
            {false, []},
            OldValidations
        ),
    case Found of
        true ->
            {ok, lists:reverse(RevNewValidations)};
        false ->
            {error, not_found}
    end.

delete(OldValidations, Name) ->
    {Found, RevNewValidations} =
        lists:foldl(
            fun
                (#{<<"name">> := NameIn}, {_FoundIn, Acc}) when NameIn =:= Name ->
                    {true, Acc};
                (Val, {FoundIn, Acc}) ->
                    {FoundIn, [Val | Acc]}
            end,
            {false, []},
            OldValidations
        ),
    case Found of
        true ->
            {ok, lists:reverse(RevNewValidations)};
        false ->
            {error, not_found}
    end.

reorder(Validations, Order) ->
    Context = #{
        not_found => sets:new([{version, 2}]),
        duplicated => sets:new([{version, 2}]),
        res => [],
        seen => sets:new([{version, 2}])
    },
    reorder(Validations, Order, Context).

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
reorder(RemainingValidations, [Name | Rest], Context0 = #{seen := Seen0}) ->
    case sets:is_element(Name, Seen0) of
        true ->
            Context = maps:update_with(
                duplicated, fun(S) -> sets:add_element(Name, S) end, Context0
            ),
            reorder(RemainingValidations, Rest, Context);
        false ->
            case safe_take(Name, RemainingValidations) of
                error ->
                    Context = maps:update_with(
                        not_found, fun(S) -> sets:add_element(Name, S) end, Context0
                    ),
                    reorder(RemainingValidations, Rest, Context);
                {ok, {Validation, Front, Rear}} ->
                    Context1 = maps:update_with(
                        seen, fun(S) -> sets:add_element(Name, S) end, Context0
                    ),
                    Context = maps:update_with(res, fun(Vs) -> [Validation | Vs] end, Context1),
                    reorder(Front ++ Rear, Rest, Context)
            end
    end.

fetch_with_index([{Pos, #{name := Name} = Validation} | _Rest], Name) ->
    {Pos, Validation};
fetch_with_index([{_, _} | Rest], Name) ->
    fetch_with_index(Rest, Name);
fetch_with_index(Validations, Name) ->
    fetch_with_index(lists:enumerate(Validations), Name).

safe_take(Name, Validations) ->
    case lists:splitwith(fun(#{<<"name">> := N}) -> N =/= Name end, Validations) of
        {_Front, []} ->
            error;
        {Front, [Found | Rear]} ->
            {ok, {Found, Front, Rear}}
    end.

do_lookup(_Name, _Validations = []) ->
    {error, not_found};
do_lookup(Name, [#{name := Name} = Validation | _Rest]) ->
    {ok, Validation};
do_lookup(Name, [_ | Rest]) ->
    do_lookup(Name, Rest).

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
