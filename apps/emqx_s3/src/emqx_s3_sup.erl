%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_sup).

-behaviour(supervisor).

-include_lib("emqx/include/types.hrl").

-export([
    start_link/0,
    start_profile/2,
    stop_profile/1
]).

-export([init/1]).

-spec start_link() -> emqx_types:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_profile(emqx_s3:profile_id(), emqx_s3:profile_config()) -> supervisor:startchild_ret().
start_profile(ProfileId, ProfileConfig) ->
    supervisor:start_child(?MODULE, emqx_s3_profile_sup:child_spec(ProfileId, ProfileConfig)).

-spec stop_profile(emqx_s3:profile_id()) -> ok_or_error(term()).
stop_profile(ProfileId) ->
    case supervisor:terminate_child(?MODULE, ProfileId) of
        ok ->
            supervisor:delete_child(?MODULE, ProfileId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% supervisor callbacks
%%-------------------------------------------------------------------

init([]) ->
    ok = emqx_s3_profile_http_pools:create_table(),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    {ok, {SupFlags, []}}.
