%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_utils).

-export([split_insert_sql/1]).

%% SQL = <<"INSERT INTO \"abc\" (c1,c2,c3) VALUES (${1}, ${1}, ${1})">>
split_insert_sql(SQL) ->
    case re:split(SQL, "((?i)values)", [{return, binary}]) of
        [Part1, _, Part3] ->
            case string:trim(Part1, leading) of
                <<"insert", _/binary>> = InsertSQL ->
                    {ok, {InsertSQL, Part3}};
                <<"INSERT", _/binary>> = InsertSQL ->
                    {ok, {InsertSQL, Part3}};
                _ ->
                    {error, not_insert_sql}
            end;
        _ ->
            {error, not_insert_sql}
    end.
