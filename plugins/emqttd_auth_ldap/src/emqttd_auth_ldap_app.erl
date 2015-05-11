<<<<<<< HEAD
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
%%% ldap authentication app.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth_ldap_app).

-behaviour(application).
%% Application callbacks
-export([start/2, prep_stop/1, stop/1]).

-behaviour(supervisor).
%% Supervisor callbacks
-export([init/1]).

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

start(_StartType, _StartArgs) ->
    Opts = application:get_all_env(emqttd_auth_ldap, ldap),
    emqttd_access_control:register_mod(auth, emqttd_auth_ldap, Opts),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

prep_stop(State) ->
    emqttd_access_control:unregister_mod(auth, emqttd_auth_ldap), State.

stop(_State) ->
    ok.

%%%=============================================================================
%%% Supervisor callbacks
%%%=============================================================================

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.

=======
-module(emqttd_auth_ldap_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqttd_auth_ldap_sup:start_link(),
    Env = application:get_all_env(),
    emqttd_access_control:register_mod(auth, emqttd_auth_ldap, Env),
    {ok, Sup}.

stop(_State) ->
    emqttd_access_control:unregister_mod(auth, emqttd_auth_ldap),
    ok.
>>>>>>> c6e92388798302ae3b44286f444505d1c385aba7
