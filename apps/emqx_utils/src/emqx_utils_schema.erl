%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_schema).

-export([naive_env_interpolation/1]).

%% @doc This function helps to perform a naive string interpolation which
%% only looks at the first segment of the string and tries to replace it.
%% For example
%%  "$MY_FILE_PATH"
%%  "${MY_FILE_PATH}"
%%  "$ENV_VARIABLE/sub/path"
%%  "${ENV_VARIABLE}/sub/path"
%%  "${ENV_VARIABLE}\sub\path" # windows
%% This function returns undefined if the input is undefined
%% otherwise always return string.
naive_env_interpolation(undefined) ->
    undefined;
naive_env_interpolation(Bin) when is_binary(Bin) ->
    naive_env_interpolation(unicode:characters_to_list(Bin, utf8));
naive_env_interpolation("$" ++ Maybe = Original) ->
    {Env, Tail} = split_path(Maybe),
    case resolve_env(Env) of
        {ok, Path} ->
            filename:join([Path, Tail]);
        error ->
            logger:log(warning, #{
                msg => "cannot_resolve_env_variable",
                env => Env,
                original => Original
            }),
            Original
    end;
naive_env_interpolation(Other) ->
    Other.

split_path(Path) ->
    {Name0, Tail} = split_path(Path, []),
    {string:trim(Name0, both, "{}"), Tail}.

split_path([], Acc) ->
    {lists:reverse(Acc), []};
split_path([Char | Rest], Acc) when Char =:= $/ orelse Char =:= $\\ ->
    {lists:reverse(Acc), string:trim(Rest, leading, "/\\")};
split_path([Char | Rest], Acc) ->
    split_path(Rest, [Char | Acc]).

resolve_env(Name) ->
    Value = os:getenv(Name),
    case Value =/= false andalso Value =/= "" of
        true ->
            {ok, Value};
        false ->
            special_env(Name)
    end.

-ifdef(TEST).
%% when running tests, we need to mock the env variables
special_env("EMQX_ETC_DIR") ->
    {ok, filename:join([code:lib_dir(emqx), etc])};
special_env("EMQX_LOG_DIR") ->
    {ok, "log"};
special_env(_Name) ->
    %% only in tests
    error.
-else.
special_env(_Name) -> error.
-endif.
