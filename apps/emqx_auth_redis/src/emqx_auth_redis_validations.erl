%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_redis_validations).

-export([
    validate_command/2
]).

validate_command([], _Command) ->
    ok;
validate_command([Validation | Rest], Command) ->
    case validate(Validation, Command) of
        ok ->
            validate_command(Rest, Command);
        {error, _} = Error ->
            Error
    end.

validate(not_empty, []) ->
    {error, empty_command};
validate(not_empty, _) ->
    ok;
validate({command_name, AllowedNames}, [Name | _]) ->
    IsAllowed = lists:any(
        fun(AllowedName) ->
            string:equal(AllowedName, Name, true, none)
        end,
        AllowedNames
    ),
    case IsAllowed of
        true ->
            ok;
        false ->
            {error, {invalid_command_name, Name}}
    end;
validate({command_name, _}, _) ->
    {error, invalid_command_name};
validate({allowed_fields, AllowedFields}, [_CmdName, _CmdKey | Args]) ->
    Unknown = lists:filter(fun(Arg) -> not lists:member(Arg, AllowedFields) end, Args),
    case Unknown of
        [] ->
            ok;
        _ ->
            {error, {unknown_fields, Unknown}}
    end;
validate({allowed_fields, _}, _) ->
    ok;
validate({required_field_one_of, Required}, [_CmdName, _CmdKey | Args]) ->
    HasRequired = lists:any(fun(Field) -> lists:member(Field, Args) end, Required),
    case HasRequired of
        true ->
            ok;
        false ->
            {error, {missing_required_field, Required}}
    end;
validate({required_field_one_of, Required}, _) ->
    {error, {missing_required_field, Required}}.
