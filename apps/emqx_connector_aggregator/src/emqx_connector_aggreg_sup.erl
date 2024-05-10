%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_sup).

-export([
    start_link/0,
    start_child/1,
    delete_child/1
]).

-behaviour(supervisor).
-export([init/1]).

-define(SUPREF, ?MODULE).

%%

start_link() ->
    supervisor:start_link({local, ?SUPREF}, ?MODULE, root).

start_child(ChildSpec) ->
    supervisor:start_child(?SUPREF, ChildSpec).

delete_child(ChildId) ->
    case supervisor:terminate_child(?SUPREF, ChildId) of
        ok ->
            supervisor:delete_child(?SUPREF, ChildId);
        Error ->
            Error
    end.

%%

init(root) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 1,
        period => 1
    },
    {ok, {SupFlags, []}}.
