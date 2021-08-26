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
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([ roots/0
        , fields/1
        ]).

-export([ create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

roots() -> [config].

fields(config) ->
    [ {type,                    {enum, ['password-based:mysql']}}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_position,           fun salt_position/1}
    , {query,                   fun query/1}
    , {query_timeout,           fun query_timeout/1}
    ] ++ emqx_authn_schema:common_fields()
    ++ emqx_connector_schema_lib:relational_db_fields()
    ++ emqx_connector_schema_lib:ssl_fields().

password_hash_algorithm(type) -> {enum, [plain, md5, sha, sha256, sha512, bcrypt]};
password_hash_algorithm(default) -> sha256;
password_hash_algorithm(_) -> undefined.

salt_position(type) -> {enum, [prefix, suffix]};
salt_position(default) -> prefix;
salt_position(_) -> undefined.

query(type) -> string();
query(nullable) -> false;
query(_) -> undefined.

query_timeout(type) -> integer();
query_timeout(default) -> 5000;
query_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{ password_hash_algorithm := Algorithm
        , salt_position := SaltPosition
        , query := Query0
        , query_timeout := QueryTimeout
        , '_unique' := Unique
        } = Config) ->
    {Query, PlaceHolders} = parse_query(Query0),
    State = #{password_hash_algorithm => Algorithm,
              salt_position => SaltPosition,
              query => Query,
              placeholders => PlaceHolders,
              query_timeout => QueryTimeout,
              '_unique' => Unique},
    case emqx_resource:create_local(Unique, emqx_connector_mysql, Config) of
        {ok, already_created} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

update(Config, State) ->
    case create(Config) of
        {ok, NewState} ->
            ok = destroy(State),
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := Password} = Credential,
             #{placeholders := PlaceHolders,
               query := Query,
               query_timeout := Timeout,
               '_unique' := Unique} = State) ->
    try
        Params = emqx_authn_utils:replace_placeholders(PlaceHolders, Credential),
        case emqx_resource:query(Unique, {sql, Query, Params, Timeout}) of
            {ok, _Columns, []} -> ignore;
            {ok, Columns, Rows} ->
                Selected = maps:from_list(lists:zip(Columns, Rows)),
                case check_password(Password, Selected, State) of
                    ok ->
                        {ok, #{superuser => maps:get(<<"superuser">>, Selected, false)}};
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, _Reason} ->
                ignore
        end
    catch
        error:Error ->
            ?LOG(warning, "The following error occurred in '~s' during authentication: ~p", [Unique, Error]),
            ignore
    end.

destroy(#{'_unique' := Unique}) ->
    _ = emqx_resource:remove_local(Unique),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

check_password(undefined, _Selected, _State) ->
    {error, bad_username_or_password};
check_password(Password,
               #{<<"password_hash">> := Hash},
               #{password_hash_algorithm := bcrypt}) ->
    case {ok, Hash} =:= bcrypt:hashpw(Password, Hash) of
        true -> ok;
        false -> {error, bad_username_or_password}
    end;
check_password(Password,
               #{<<"password_hash">> := Hash} = Selected,
               #{password_hash_algorithm := Algorithm,
                 salt_position := SaltPosition}) ->
    Salt = maps:get(<<"salt">>, Selected, <<>>),
    case Hash =:= emqx_authn_utils:hash(Algorithm, Password, Salt, SaltPosition) of
        true -> ok;
        false -> {error, bad_username_or_password}
    end.

%% TODO: Support prepare
parse_query(Query) ->
    case re:run(Query, ?RE_PLACEHOLDER, [global, {capture, all, binary}]) of
        {match, Captured} ->
            PlaceHolders = [PlaceHolder || PlaceHolder <- Captured],
            NQuery = re:replace(Query, "'\\$\\{[a-z0-9\\_]+\\}'", "?", [global, {return, binary}]),
            {NQuery, PlaceHolders};
        nomatch ->
            {Query, []}
    end.
