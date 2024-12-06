%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_secret_loader).

%% API
-export([load/1]).
-export([file/1]).

-export_type([source/0]).

-type source() :: {file, string() | binary()}.

-spec load(source()) -> binary() | no_return().
load({file, <<"file://", Path/binary>>}) ->
    file(Path);
load({file, "file://" ++ Path}) ->
    file(Path).

-spec file(file:filename_all()) -> binary() | no_return().
file(Filename0) ->
    Filename = emqx_schema:naive_env_interpolation(Filename0),
    case file:read_file(Filename) of
        {ok, Secret} ->
            string:trim(Secret, trailing);
        {error, Reason} ->
            throw(#{
                msg => failed_to_read_secret_file,
                path => Filename,
                reason => emqx_utils:explain_posix(Reason)
            })
    end.
