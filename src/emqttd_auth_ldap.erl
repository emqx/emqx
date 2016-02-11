%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc LDAP Authentication Module
-module(emqttd_auth_ldap).

-include("emqttd.hrl").

-import(proplists, [get_value/2, get_value/3]).

-behaviour(emqttd_auth_mod).

-export([init/1, check/3, description/0]).

-record(state, {servers, user_dn, options}).

init(Opts) ->
    Servers = get_value(servers, Opts, ["localhost"]),
    Port    = get_value(port, Opts, 389),
    Timeout = get_value(timeout, Opts, 30),
    UserDn  = get_value(user_dn, Opts),
    LdapOpts =
    case get_value(ssl, Opts, false) of
        true -> 
            SslOpts = get_value(sslopts, Opts),
            [{port, Port}, {timeout, Timeout}, {sslopts, SslOpts}];
        false ->
            [{port, Port}, {timeout, Timeout}]
    end,
    {ok, #state{servers = Servers, user_dn = UserDn, options = LdapOpts}}.

check(#mqtt_client{username = undefined}, _Password, _State) ->
    {error, username_undefined};
check(_Client, undefined, _State) ->
    {error, password_undefined};
check(_Client, <<>>, _State) ->
    {error, password_undefined};
check(#mqtt_client{username = Username}, Password,
      #state{servers = Servers, user_dn = UserDn, options = Options}) ->
    case eldap:open(Servers, Options) of
        {ok, LDAP} ->
            UserDn1 = fill(binary_to_list(Username), UserDn),
            ldap_bind(LDAP, UserDn1, binary_to_list(Password));
        {error, Reason} ->
            {error, Reason}
    end.

ldap_bind(LDAP, UserDn, Password) ->
    case catch eldap:simple_bind(LDAP, UserDn, Password) of
        ok ->
            ok;
        {error, invalidCredentials} ->
            {error, invalid_credentials};
        {error, Error} ->
            {error, Error};
        {'EXIT', Reason} ->
            {error, Reason}
    end.

fill(Username, UserDn) ->
    re:replace(UserDn, "\\$u", Username, [global, {return, list}]).

description() -> "LDAP Authentication Module".

