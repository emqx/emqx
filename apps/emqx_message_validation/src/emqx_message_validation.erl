%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_validation).

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
    move/2,
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

%% Internal exports
-export([parse_sql_check/1]).

%% Internal functions; exported for tests
-export([
    evaluate_sql_check/3
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(TRACE_TAG, "MESSAGE_VALIDATION").
-define(CONF_ROOT, message_validation).
-define(VALIDATIONS_CONF_PATH, [?CONF_ROOT, validations]).

-type validation_name() :: binary().
-type validation() :: _TODO.
-type position() :: front | rear | {'after', validation_name()} | {before, validation_name()}.

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
    lists:foreach(fun insert/1, emqx:get_config(?VALIDATIONS_CONF_PATH, [])).

unload() ->
    lists:foreach(fun delete/1, emqx:get_config(?VALIDATIONS_CONF_PATH, [])).

-spec list() -> [validation()].
list() ->
    emqx:get_config(?VALIDATIONS_CONF_PATH, []).

-spec move(validation_name(), position()) ->
    {ok, _} | {error, _}.
move(Name, Position) ->
    emqx:update_config(
        ?VALIDATIONS_CONF_PATH,
        {move, Name, Position},
        #{override_to => cluster}
    ).

-spec lookup(validation_name()) -> {ok, validation()} | {error, not_found}.
lookup(Name) ->
    Validations = emqx:get_config(?VALIDATIONS_CONF_PATH, []),
    do_lookup(Name, Validations).

-spec insert(validation()) ->
    {ok, _} | {error, _}.
insert(Validation) ->
    emqx:update_config(
        ?VALIDATIONS_CONF_PATH,
        {append, Validation},
        #{override_to => cluster}
    ).

-spec update(validation()) ->
    {ok, _} | {error, _}.
update(Validation) ->
    emqx:update_config(
        ?VALIDATIONS_CONF_PATH,
        {update, Validation},
        #{override_to => cluster}
    ).

-spec delete(validation_name()) ->
    {ok, _} | {error, _}.
delete(Name) ->
    emqx:update_config(
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
    case emqx_message_validation_registry:matching_validations(Topic) of
        [] ->
            ok;
        Validations ->
            case run_validations(Validations, Message) of
                ok ->
                    {ok, Message};
                drop ->
                    {stop, Message#message{headers = Headers#{allow_publish => false}}};
                disconnect ->
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
pre_config_update(?VALIDATIONS_CONF_PATH, {move, Name, Position}, OldValidations) ->
    move(OldValidations, Name, Position).

post_config_update(?VALIDATIONS_CONF_PATH, {append, #{<<"name">> := Name}}, New, _Old, _AppEnvs) ->
    {Pos, Validation} = fetch_with_index(New, Name),
    ok = emqx_message_validation_registry:insert(Pos, Validation),
    ok;
post_config_update(?VALIDATIONS_CONF_PATH, {update, #{<<"name">> := Name}}, New, Old, _AppEnvs) ->
    {_Pos, OldValidation} = fetch_with_index(Old, Name),
    {Pos, NewValidation} = fetch_with_index(New, Name),
    ok = emqx_message_validation_registry:update(OldValidation, Pos, NewValidation),
    ok;
post_config_update(?VALIDATIONS_CONF_PATH, {delete, Name}, _New, Old, _AppEnvs) ->
    {_Pos, Validation} = fetch_with_index(Old, Name),
    ok = emqx_message_validation_registry:delete(Validation),
    ok;
post_config_update(?VALIDATIONS_CONF_PATH, {move, _Name, _Position}, New, _Old, _AppEnvs) ->
    ok = emqx_message_validation_registry:reindex_positions(New),
    ok.

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
    #{
        name := Name,
        log_failure_at := FailureLogLevel
    } = Validation,
    {Data, _} = emqx_rule_events:eventmsg_publish(Message),
    try emqx_rule_runtime:evaluate_select(Fields, Data, Conditions) of
        {ok, _} ->
            true;
        false ->
            false
    catch
        throw:Reason ->
            ?TRACE(
                FailureLogLevel,
                ?TRACE_TAG,
                "validation_sql_check_throw",
                #{
                    validation => Name,
                    reason => Reason
                }
            ),
            false;
        Class:Error:Stacktrace ->
            ?TRACE(
                FailureLogLevel,
                ?TRACE_TAG,
                "validation_sql_check_failure",
                #{
                    validation => Name,
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }
            ),
            false
    end.

evaluate_schema_check(Check, Validation, #message{payload = Data}) ->
    #{schema := SerdeName} = Check,
    #{
        name := Name,
        log_failure_at := FailureLogLevel
    } = Validation,
    ExtraArgs =
        case Check of
            #{type := protobuf, message_name := MessageName} ->
                [MessageName];
            _ ->
                []
        end,
    try
        emqx_schema_registry_serde:handle_rule_function(schema_check, [SerdeName, Data | ExtraArgs])
    catch
        error:{serde_not_found, _} ->
            ?TRACE(
                FailureLogLevel,
                ?TRACE_TAG,
                "validation_schema_check_schema_not_found",
                #{
                    validation => Name,
                    schema_name => SerdeName
                }
            ),
            false;
        Class:Error:Stacktrace ->
            ?TRACE(
                FailureLogLevel,
                ?TRACE_TAG,
                "validation_schema_check_failure",
                #{
                    validation => Name,
                    schema_name => SerdeName,
                    kind => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }
            ),
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

move(OldValidations, Name, front) ->
    {Validation, Front, Rear} = take(Name, OldValidations),
    {ok, [Validation | Front ++ Rear]};
move(OldValidations, Name, rear) ->
    {Validation, Front, Rear} = take(Name, OldValidations),
    {ok, Front ++ Rear ++ [Validation]};
move(OldValidations, Name, {'after', OtherName}) ->
    {Validation, Front1, Rear1} = take(Name, OldValidations),
    {OtherValidation, Front2, Rear2} = take(OtherName, Front1 ++ Rear1),
    {ok, Front2 ++ [OtherValidation, Validation] ++ Rear2};
move(OldValidations, Name, {before, OtherName}) ->
    {Validation, Front1, Rear1} = take(Name, OldValidations),
    {OtherValidation, Front2, Rear2} = take(OtherName, Front1 ++ Rear1),
    {ok, Front2 ++ [Validation, OtherValidation] ++ Rear2}.

fetch_with_index([{Pos, #{name := Name} = Validation} | _Rest], Name) ->
    {Pos, Validation};
fetch_with_index([{_, _} | Rest], Name) ->
    fetch_with_index(Rest, Name);
fetch_with_index(Validations, Name) ->
    fetch_with_index(lists:enumerate(Validations), Name).

take(Name, Validations) ->
    case lists:splitwith(fun(#{<<"name">> := N}) -> N =/= Name end, Validations) of
        {_Front, []} ->
            throw({validation_not_found, Name});
        {Front, [Found | Rear]} ->
            {Found, Front, Rear}
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
            #{
                name := Name,
                log_failure_at := FailureLogLevel
            } = Validation,
            case run_validation(Validation, Message) of
                ok ->
                    {cont, Acc};
                FailureAction ->
                    ?TRACE(
                        FailureLogLevel,
                        ?TRACE_TAG,
                        "validation_failed",
                        #{validation => Name, action => FailureAction}
                    ),
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
