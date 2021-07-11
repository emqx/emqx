%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mysql).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0, fields/1 ]).

-export([ create/3
        , update/4
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

structs() -> [config].

fields(config) ->
    [ {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_position,           {enum, [prefix, suffix]}}
    , {query,                   fun query/1}
    , {query_timeout,           fun query_timeout/1}
    ] ++ emqx_connector_schema_lib:relational_db_fields()
    ++ emqx_connector_schema_lib:ssl_fields().

password_hash_algorithm(type) -> string();
password_hash_algorithm(_) -> undefined.

query(type) -> string();
query(nullable) -> false;
query(_) -> undefined.

query_timeout(type) -> integer();
query_timeout(defualt) -> 5000;
query_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(ChainID, ServiceName, #{query := Query0,
                               password_hash_algorithm := Algorithm} = Config) ->
    {Query, PlaceHolders} = parse_query(Query0),
    ResourceID = iolist_to_binary(io_lib:format("~s/~s",[ChainID, ServiceName])),
    State = #{query => Query,
              placeholders => PlaceHolders,
              password_hash_algorithm => Algorithm},
    case emqx_resource:create_local(ResourceID, emqx_connector_mysql, Config) of
        {ok, _} ->
            {ok, State#{resource_id => ResourceID}};
        {error, already_created} ->
            {ok, State#{resource_id => ResourceID}};
        {error, Reason} ->
            {error, Reason}
    end.

update(_ChainID, _ServiceName, Config, #{resource_id := ResourceID} = State) ->
    case emqx_resource:update_local(ResourceID, emqx_connector_mysql, Config, []) of
        {ok, _} -> {ok, State};
        {error, Reason} -> {error, Reason}
    end.

authenticate(#{password := Password} = ClientInfo,
             #{resource_id := ResourceID,
               placeholders := PlaceHolders,
               query := Query,
               query_timeout := Timeout} = State) ->
    Params = emqx_authn_utils:replace_placeholder(PlaceHolders, ClientInfo),
    case emqx_resource:query(ResourceID, {sql, Query, Params, Timeout}) of
        {ok, _Columns, []} -> ignore;
        {ok, Columns, Rows} ->
            %% TODO: Support superuser
            Selected = maps:from_list(lists:zip(Columns, Rows)),
            check_password(Password, Selected, State);
        {error, _Reason} ->
            ignore
    end.

destroy(#{resource_id := ResourceID}) ->
    _ = emqx_resource:remove_local(ResourceID),
    ok.
    
%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

check_password(undefined, _Algorithm, _Selected) ->
    {stop, bad_password};
check_password(Password,
               #{password_hash := Hash},
               #{password_hash_algorithm := bcrypt}) ->
    {ok, Hash0} = bcrypt:hashpw(Password, Hash),
    case list_to_binary(Hash0) =:= Hash of
        true -> ok;
        false -> {stop, bad_password}
    end;
check_password(Password,
               #{password_hash := Hash} = Selected,
               #{password_hash_algorithm := Algorithm,
                 salt_position := SaltPosition}) ->
    Salt = maps:get(salt, Selected, <<>>),
    Hash0 = case SaltPosition of
                prefix -> emqx_passwd:hash(Algorithm, <<Salt/binary, Password/binary>>);
                suffix -> emqx_passwd:hash(Algorithm, <<Password/binary, Salt/binary>>)
            end,
    case Hash0 =:= Hash of
        true -> ok;
        false -> {stop, bad_password}
    end.

%% TODO: Support prepare
parse_query(Query) ->
    case re:run(Query, "\\$\\{[a-z0-9\\_]+\\}", [global, {capture, all, binary}]) of
        {match, Captured} ->
            PlaceHolders = [PlaceHolder || PlaceHolder <- Captured],
            NQuery = re:replace(Query, "'\\$\\{[a-z0-9\\_]+\\}'", "?", [global, {return, binary}]),
            {NQuery, PlaceHolders};
        nomatch ->
            {Query, []}
    end.
