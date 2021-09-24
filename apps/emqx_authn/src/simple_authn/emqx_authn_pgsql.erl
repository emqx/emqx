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

-module(emqx_authn_pgsql).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([ namespace/0
        , roots/0
        , fields/1
        ]).

-export([ refs/0
        , create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn:password-based:postgresql".

roots() -> [config].

fields(config) ->
    [ {mechanism,               {enum, ['password-based']}}
    , {backend,                 {enum, [postgresql]}}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_position,           {enum, [prefix, suffix]}}
    , {query,                   fun query/1}
    ] ++ emqx_authn_schema:common_fields()
    ++ emqx_connector_schema_lib:relational_db_fields()
    ++ emqx_connector_schema_lib:ssl_fields().

password_hash_algorithm(type) -> {enum, [plain, md5, sha, sha256, sha512, bcrypt]};
password_hash_algorithm(default) -> sha256;
password_hash_algorithm(_) -> undefined.

query(type) -> string();
query(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, config)].

create(#{ query := Query0
        , password_hash_algorithm := Algorithm
        , salt_position := SaltPosition
        , '_unique' := Unique
        } = Config) ->
    {Query, PlaceHolders} = parse_query(Query0),
    State = #{query => Query,
              placeholders => PlaceHolders,
              password_hash_algorithm => Algorithm,
              salt_position => SaltPosition,
              '_unique' => Unique},
    case emqx_resource:create_local(Unique, emqx_connector_pgsql, Config) of
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
             #{query := Query,
               placeholders := PlaceHolders,
               '_unique' := Unique} = State) ->
    Params = emqx_authn_utils:replace_placeholders(PlaceHolders, Credential),
    case emqx_resource:query(Unique, {sql, Query, Params}) of
        {ok, _Columns, []} -> ignore;
        {ok, Columns, Rows} ->
            NColumns = [Name || #column{name = Name} <- Columns],
            Selected = maps:from_list(lists:zip(NColumns, Rows)),
            case check_password(Password, Selected, State) of
                ok ->
                    {ok, #{is_superuser => maps:get(<<"is_superuser">>, Selected, false)}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _Reason} ->
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
            PlaceHolders = [PlaceHolder || [PlaceHolder] <- Captured],
            Replacements = ["$" ++ integer_to_list(I) || I <- lists:seq(1, length(Captured))],
            NQuery = lists:foldl(fun({PlaceHolder, Replacement}, Query0) ->
                                     re:replace(Query0, <<"'\\", PlaceHolder/binary, "'">>, Replacement, [{return, binary}])
                                 end, Query, lists:zip(PlaceHolders, Replacements)),
            {NQuery, PlaceHolders};
        nomatch ->
            {Query, []}
    end.
