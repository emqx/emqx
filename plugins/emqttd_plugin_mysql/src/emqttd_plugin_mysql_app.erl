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
%%% emqttd mysql authentication app.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_plugin_mysql_app).
-on_load(init/0).
-behaviour(application).
%% Application callbacks
-export([start/2, prep_stop/1, stop/1, nif_pbkdf2_check/2]).

-behaviour(supervisor).
%% Supervisor callbacks
-export([init/1]).
-define(NOT_LOADED, not_loaded(?LINE)).


%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

start(_StartType, _StartArgs) ->
  Env = application:get_all_env(),
  emqttd_access_control:register_mod(auth, emqttd_auth_mysql, Env),
  emqttd_access_control:register_mod(acl, emqttd_acl_mysql, Env),
  crypto:start(),
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

prep_stop(State) ->
  emqttd_access_control:unregister_mod(auth, emqttd_auth_mysql), State,
  emqttd_access_control:unregister_mod(acl, emqttd_acl_mysql), State,
  crypto:stop().

stop(_State) ->
  ok.

init() ->
  PrivDir = case code:priv_dir(?MODULE) of
              {error, _} ->
                EbinDir = filename:dirname(code:which(?MODULE)),
                AppPath = filename:dirname(EbinDir),
                filename:join(AppPath, "priv");
              Path ->
                Path
            end,
  erlang:load_nif(filename:join(PrivDir, "emqttd_plugin_mysql_app"), 0).

%%%=============================================================================
%%% Supervisor callbacks(Dummy)
%%%=============================================================================

init([]) ->
  {ok, {{one_for_one, 5, 10}, []}}.

not_loaded(Line) ->
  erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

nif_pbkdf2_check(Password, Hash) ->
  ?NOT_LOADED.
