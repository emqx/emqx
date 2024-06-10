%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_config).

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

%% `emqx_config_handler' API
-export([pre_config_update/3, post_config_update/5]).

%% `emqx_config_backup' API
-behaviour(emqx_config_backup).
-export([import_config/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(CONF_ROOT, schema_validation).
-define(CONF_ROOT_BIN, <<"schema_validation">>).
-define(VALIDATIONS_CONF_PATH, [?CONF_ROOT, validations]).

-type validation_name() :: emqx_schema_validation:validation_name().
-type validation() :: emqx_schema_validation:validation().
-type raw_validation() :: #{binary() => _}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec add_handler() -> ok.
add_handler() ->
    ok = emqx_config_handler:add_handler([?CONF_ROOT], ?MODULE),
    ok = emqx_config_handler:add_handler(?VALIDATIONS_CONF_PATH, ?MODULE),
    ok.

-spec remove_handler() -> ok.
remove_handler() ->
    ok = emqx_config_handler:remove_handler(?VALIDATIONS_CONF_PATH),
    ok = emqx_config_handler:remove_handler([?CONF_ROOT]),
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

-spec insert(raw_validation()) ->
    {ok, _} | {error, _}.
insert(Validation) ->
    emqx_conf:update(
        ?VALIDATIONS_CONF_PATH,
        {append, Validation},
        #{override_to => cluster}
    ).

-spec update(raw_validation()) ->
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
    reorder(OldValidations, Order);
pre_config_update([?CONF_ROOT], {merge, NewConfig}, OldConfig) ->
    #{resulting_config := Config} = prepare_config_merge(NewConfig, OldConfig),
    {ok, Config};
pre_config_update([?CONF_ROOT], {replace, NewConfig}, _OldConfig) ->
    {ok, NewConfig}.

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
    ok;
post_config_update([?CONF_ROOT], {merge, _}, ResultingConfig, Old, _AppEnvs) ->
    #{validations := ResultingValidations} = ResultingConfig,
    #{validations := OldValidations} = Old,
    #{added := NewValidations0} =
        emqx_utils:diff_lists(
            ResultingValidations,
            OldValidations,
            fun(#{name := N}) -> N end
        ),
    NewValidations =
        lists:map(
            fun(#{name := Name}) ->
                {Pos, Validation} = fetch_with_index(ResultingValidations, Name),
                ok = emqx_schema_validation_registry:insert(Pos, Validation),
                #{name => Name, pos => Pos}
            end,
            NewValidations0
        ),
    {ok, #{new_validations => NewValidations}};
post_config_update([?CONF_ROOT], {replace, Input}, ResultingConfig, Old, _AppEnvs) ->
    #{
        new_validations := NewValidations,
        changed_validations := ChangedValidations0,
        deleted_validations := DeletedValidations
    } = prepare_config_replace(Input, Old),
    #{validations := ResultingValidations} = ResultingConfig,
    #{validations := OldValidations} = Old,
    lists:foreach(
        fun(Name) ->
            {_Pos, Validation} = fetch_with_index(OldValidations, Name),
            ok = emqx_schema_validation_registry:delete(Validation)
        end,
        DeletedValidations
    ),
    lists:foreach(
        fun(Name) ->
            {Pos, Validation} = fetch_with_index(ResultingValidations, Name),
            ok = emqx_schema_validation_registry:insert(Pos, Validation)
        end,
        NewValidations
    ),
    ChangedValidations =
        lists:map(
            fun(Name) ->
                {_Pos, OldValidation} = fetch_with_index(OldValidations, Name),
                {Pos, NewValidation} = fetch_with_index(ResultingValidations, Name),
                ok = emqx_schema_validation_registry:update(OldValidation, Pos, NewValidation),
                #{name => Name, pos => Pos}
            end,
            ChangedValidations0
        ),
    ok = emqx_schema_validation_registry:reindex_positions(ResultingValidations),
    {ok, #{changed_validations => ChangedValidations}}.

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
            ChangedPaths = Keys0 -- [<<"validations">>],
            {ok, #{root_key => ?CONF_ROOT, changed => ChangedPaths}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_ROOT, changed => []}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

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

%% "Merging" in the context of the validation array means:
%%   * Existing validations (identified by `name') are left untouched.
%%   * No validations are removed.
%%   * New validations are appended to the existing list.
%%   * Existing validations are not reordered.
prepare_config_merge(NewConfig0, OldConfig) ->
    {ImportedRawValidations, NewConfigNoValidations} =
        case maps:take(<<"validations">>, NewConfig0) of
            error ->
                {[], NewConfig0};
            {V, R} ->
                {V, R}
        end,
    OldRawValidations = maps:get(<<"validations">>, OldConfig, []),
    #{added := NewRawValidations} = emqx_utils:diff_lists(
        ImportedRawValidations,
        OldRawValidations,
        fun(#{<<"name">> := N}) -> N end
    ),
    Config0 = emqx_utils_maps:deep_merge(OldConfig, NewConfigNoValidations),
    Config = maps:update_with(
        <<"validations">>,
        fun(OldVs) -> OldVs ++ NewRawValidations end,
        NewRawValidations,
        Config0
    ),
    #{
        new_validations => NewRawValidations,
        resulting_config => Config
    }.

prepare_config_replace(NewConfig, OldConfig) ->
    ImportedRawValidations = maps:get(<<"validations">>, NewConfig, []),
    OldValidations = maps:get(validations, OldConfig, []),
    %% Since, at this point, we have an input raw config but a parsed old config, we
    %% project both to the to have only their names, and consider common names as changed.
    #{
        added := NewValidations,
        removed := DeletedValidations,
        changed := ChangedValidations0,
        identical := ChangedValidations1
    } = emqx_utils:diff_lists(
        lists:map(fun(#{<<"name">> := N}) -> N end, ImportedRawValidations),
        lists:map(fun(#{name := N}) -> N end, OldValidations),
        fun(N) -> N end
    ),
    #{
        new_validations => NewValidations,
        changed_validations => ChangedValidations0 ++ ChangedValidations1,
        deleted_validations => DeletedValidations
    }.
