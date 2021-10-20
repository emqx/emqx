%%%-------------------------------------------------------------------
%% @doc emqx_authz public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_authz_app).

-behaviour(application).

-include("emqx_authz.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = mria_rlog:wait_for_shards([?ACL_SHARDED], infinity),
    {ok, Sup} = emqx_authz_sup:start_link(),
    ok = emqx_authz:init(),
    {ok, Sup}.

stop(_State) ->
    emqx_conf:remove_handler(?CONF_KEY_PATH),
    ok.

%% internal functions
