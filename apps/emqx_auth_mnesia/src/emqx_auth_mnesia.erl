%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mnesia).

-include("emqx_auth_mnesia.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

-define(TABLE, emqx_user).
%% Auth callbacks
-export([ init/1
        , register_metrics/0
        , check/3
        , description/0
        ]).

init(#{clientid_list := ClientidList, username_list := UsernameList}) ->
    ok = ekka_mnesia:create_table(emqx_user, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, emqx_user)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    _ = [ add_default_user({{clientid, iolist_to_binary(Clientid)}, iolist_to_binary(Password)})
      || {Clientid, Password} <- ClientidList],
    _ = [ add_default_user({{username, iolist_to_binary(Username)}, iolist_to_binary(Password)})
      || {Username, Password} <- UsernameList],
    ok = ekka_mnesia:copy_table(emqx_user, disc_copies).

%% @private
add_default_user({Login, Password}) when is_tuple(Login) ->
    emqx_auth_mnesia_cli:add_user(Login, Password).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTH_METRICS).

check(ClientInfo = #{ clientid := Clientid
                    , password := NPassword
                    }, AuthResult, #{hash_type := HashType}) ->
    Username = maps:get(username, ClientInfo, undefined),
    MatchSpec = ets:fun2ms(fun({?TABLE, {clientid, X }, Password, InterTime}) when X =:= Clientid-> Password;
                              ({?TABLE, {username, X }, Password, InterTime}) when X =:= Username andalso X =/= undefined -> Password
                           end),
    case ets:select(?TABLE, MatchSpec) of
        [] ->
            emqx_metrics:inc(?AUTH_METRICS(ignore)),
            ok;
        List ->
            case [ Hash  || <<Salt:4/binary, Hash/binary>> <- lists:sort(fun emqx_auth_mnesia_cli:comparing/2, List),
                            Hash =:= hash(NPassword, Salt, HashType)
                 ] of
                [] ->
                    ?LOG(error, "[Mnesia] Auth from mnesia failed: ~p", [ClientInfo]),
                    emqx_metrics:inc(?AUTH_METRICS(failure)),
                    {stop, AuthResult#{anonymous => false, auth_result => password_error}};
                _ ->
                    emqx_metrics:inc(?AUTH_METRICS(success)),
                    {stop, AuthResult#{anonymous => false, auth_result => success}}
            end
    end.

description() -> "Authentication with Mnesia".

hash(undefined, SaltBin, HashType) ->
    hash(<<>>, SaltBin, HashType);
hash(Password, SaltBin, HashType) ->
    emqx_passwd:hash(HashType, <<SaltBin/binary, Password/binary>>).
