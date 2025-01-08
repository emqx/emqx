%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
