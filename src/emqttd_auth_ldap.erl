%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% LDAP Authentication Module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_ldap).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqttd/include/emqttd.hrl").

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
    {error, "Username undefined"};
check(_Client, undefined, _State) ->
    {error, "Password undefined"};
check(_Client, <<>>, _State) ->
    {error, "Password undefined"};
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
            {error, "LDAP Invalid Credentials"};
        {error, Error} ->
            {error, Error};
        {'EXIT', Reason} ->
            {error, Reason}
    end.

fill(Username, UserDn) ->
    re:replace(UserDn, "\\$u", Username, [global, {return, list}]).

description() -> 
    "LDAP Authentication Module".

