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

%% Auth callbacks
-export([ init/1
        , register_metrics/0
        , check/3
        , description/0
        ]).

init(DefaultUsers) ->
    ok = ekka_mnesia:create_table(emqx_user, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, emqx_user)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    ok = lists:foreach(fun add_default_user/1, DefaultUsers),
    ok = ekka_mnesia:copy_table(emqx_user, disc_copies).

%% @private
add_default_user({Login, Password, IsSuperuser}) ->
    emqx_auth_mnesia_cli:add_user(iolist_to_binary(Login), iolist_to_binary(Password), IsSuperuser).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTH_METRICS).

check(ClientInfo = #{password := Password}, AuthResult, #{hash_type := HashType, key_as := As}) ->
    Login = maps:get(As, ClientInfo),
    case emqx_auth_mnesia_cli:lookup_user(Login) of
        [] -> 
            emqx_metrics:inc(?AUTH_METRICS(ignore)),
            ok;
        [User] ->
            case emqx_passwd:check_pass({User#emqx_user.password, Password}, HashType) of
                ok -> 
                    emqx_metrics:inc(?AUTH_METRICS(success)),
                    {stop, AuthResult#{is_superuser => is_superuser(User),
                                       anonymous => false,
                                       auth_result => success}};
                {error, Reason} -> 
                    ?LOG(error, "[Mnesia] Auth from mnesia failed: ~p", [Reason]),
                    emqx_metrics:inc(?AUTH_METRICS(failure)),
                    {stop, AuthResult#{auth_result => password_error, anonymous => false}}
            end
    end.

description() -> "Authentication with Mnesia".

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
is_superuser(#emqx_user{is_superuser = true}) ->
    true;
is_superuser(_) ->
    false.
