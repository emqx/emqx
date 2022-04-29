%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc HOCON schema help module
-module(emqx_hocon).

-export([
    format_path/1,
    check/2
]).

%% @doc Format hocon config field path to dot-separated string in iolist format.
-spec format_path([atom() | string() | binary()]) -> iolist().
format_path([]) -> "";
format_path([Name]) -> iol(Name);
format_path([Name | Rest]) -> [iol(Name), "." | format_path(Rest)].

%% @doc Plain check the input config.
%% The input can either be `richmap' or plain `map'.
%% Always return plain map with atom keys.
-spec check(module(), hocon:config() | iodata()) ->
    {ok, hocon:config()} | {error, any()}.
check(SchemaModule, Conf) when is_map(Conf) ->
    %% TODO: remove required
    %% fields should state required or not in their schema
    Opts = #{atom_key => true, required => false},
    try
        {ok, hocon_tconf:check_plain(SchemaModule, Conf, Opts)}
    catch
        throw:Reason ->
            {error, Reason}
    end;
check(SchemaModule, HoconText) ->
    case hocon:binary(HoconText, #{format => map}) of
        {ok, MapConfig} ->
            check(SchemaModule, MapConfig);
        {error, Reason} ->
            {error, Reason}
    end.

%% Ensure iolist()
iol(B) when is_binary(B) -> B;
iol(A) when is_atom(A) -> atom_to_binary(A, utf8);
iol(L) when is_list(L) -> L.
