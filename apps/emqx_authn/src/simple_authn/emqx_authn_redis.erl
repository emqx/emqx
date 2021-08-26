%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_redis).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
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

structs() -> [""].

fields("") ->
    [ {config, {union, [ hoconsc:t(standalone)
                       , hoconsc:t(cluster)
                       , hoconsc:t(sentinel)
                       ]}}
    ];

fields(standalone) ->
    common_fields() ++ emqx_connector_redis:fields(single);

fields(cluster) ->
    common_fields() ++ emqx_connector_redis:fields(cluster);

fields(sentinel) ->
    common_fields() ++ emqx_connector_redis:fields(sentinel).

common_fields() ->
    [ {name,                    fun emqx_authn_schema:authenticator_name/1}
    , {mechanism,               {enum, ['password-based']}}
    , {server_type,             {enum, [redis]}}
    , {query,                   fun query/1}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_position,           fun salt_position/1}
    ].

query(type) -> string();
query(nullable) -> false;
query(_) -> undefined.

password_hash_algorithm(type) -> {enum, [plain, md5, sha, sha256, sha512, bcrypt]};
password_hash_algorithm(default) -> sha256;
password_hash_algorithm(_) -> undefined.

salt_position(type) -> {enum, [prefix, suffix]};
salt_position(default) -> prefix;
salt_position(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{ query := Query
        , '_unique' := Unique
        } = Config) ->
    try
        NQuery = parse_query(Query),
        State = maps:with([ password_hash_algorithm
                          , salt_position
                          , '_unique'], Config),
        NState = State#{query => NQuery},
        case emqx_resource:create_local(Unique, emqx_connector_redis, Config) of
            {ok, already_created} ->
                {ok, NState};
            {ok, _} ->
                {ok, NState};
            {error, Reason} ->
                {error, Reason}
        end
    catch
        error:{unsupported_query, Query} ->
            {error, {unsupported_query, Query}};
        error:missing_password_hash ->
            {error, missing_password_hash};
        error:{unsupported_field, Field} ->
            {error, {unsupported_field, Field}}
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
             #{ query := {Command, Key, Fields}
              , '_unique' := Unique
              } = State) ->
    try
        NKey = binary_to_list(iolist_to_binary(replace_placeholders(Key, Credential))),
        case emqx_resource:query(Unique, {cmd, [Command, NKey | Fields]}) of
            {ok, Values} ->
                Selected = merge(Fields, Values),
                case check_password(Password, Selected, State) of
                   ok ->
                       {ok, #{superuser => maps:get("superuser", Selected, false)}};
                   {error, Reason} ->
                       {error, Reason}
                end;
            {error, Reason} ->
                ?LOG(error, "['~s'] Query failed: ~p", [Unique, Reason]),
                ignore
        end
    catch
        error:{cannot_get_variable, Placeholder} ->
            ?LOG(warning, "The following error occurred in '~s' during authentication: ~p", [Unique, {cannot_get_variable, Placeholder}]),
            ignore
    end.

destroy(#{'_unique' := Unique}) ->
    _ = emqx_resource:remove_local(Unique),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Only support HGET and HMGET
parse_query(Query) ->
    case string:tokens(Query, " ") of
        [Command, Key, Field | Fields] when Command =:= "HGET" orelse Command =:= "HMGET" ->
            NFields = [Field | Fields],
            check_fields(NFields),
            NKey = parse_key(Key),
            {Command, NKey, NFields};
        _ ->
            error({unsupported_query, Query})
    end.

check_fields(Fields) ->
    check_fields(Fields, false).

check_fields([], false) ->
    error(missing_password_hash);
check_fields([], true) ->
    ok;
check_fields(["password_hash" | More], false) ->
    check_fields(More, true);
check_fields(["salt" | More], HasPassHash) ->
    check_fields(More, HasPassHash);
check_fields(["superuser" | More], HasPassHash) ->
    check_fields(More, HasPassHash);
check_fields([Field | _], _) ->
    error({unsupported_field, Field}).

parse_key(Key) ->
    Tokens = re:split(Key, "(" ++ ?RE_PLACEHOLDER ++ ")", [{return, binary}, group, trim]),
    parse_key(Tokens, []).

parse_key([], Acc) ->
    lists:reverse(Acc);
parse_key([[Constant, Placeholder] | Tokens], Acc) ->
    parse_key(Tokens, [{placeholder, Placeholder}, {constant, Constant} | Acc]);
parse_key([[Constant] | Tokens], Acc) ->
    parse_key(Tokens, [{constant, Constant} | Acc]).

replace_placeholders(Key, Credential) ->
    lists:map(fun({constant, Constant}) ->
                  Constant;
                 ({placeholder, Placeholder}) ->
                  case emqx_authn_utils:replace_placeholder(Placeholder, Credential) of
                      undefined -> error({cannot_get_variable, Placeholder});
                      Value -> Value
                  end
              end, Key).

merge(Fields, Value) when not is_list(Value) ->
    merge(Fields, [Value]);
merge(Fields, Values) ->
    maps:from_list(
        lists:filter(fun({_, V}) ->
                         V =/= undefined
                     end, lists:zip(Fields, Values))).

check_password(undefined, _Selected, _State) ->
    {error, bad_username_or_password};
check_password(Password,
               #{"password_hash" := PasswordHash},
               #{password_hash_algorithm := bcrypt}) ->
    case {ok, PasswordHash} =:= bcrypt:hashpw(Password, PasswordHash) of
        true -> ok;
        false -> {error, bad_username_or_password}
    end;
check_password(Password,
               #{"password_hash" := PasswordHash} = Selected,
               #{password_hash_algorithm := Algorithm,
                 salt_position := SaltPosition}) ->
    Salt = maps:get("salt", Selected, <<>>),
    case PasswordHash =:= emqx_authn_utils:hash(Algorithm, Password, Salt, SaltPosition) of
        true -> ok;
        false -> {error, bad_username_or_password}
    end;
check_password(_Password, _Selected, _State) ->
    ignore.
