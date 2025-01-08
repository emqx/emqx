%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_sup).

-behaviour(supervisor).

-include_lib("emqx/include/types.hrl").

-export([
    start_link/2,
    child_spec/2
]).

-export([init/1]).

-spec start_link(emqx_s3:profile_id(), emqx_s3:profile_config()) -> emqx_types:startlink_ret().
start_link(ProfileId, ProfileConfig) ->
    supervisor:start_link(?MODULE, [ProfileId, ProfileConfig]).

-spec child_spec(emqx_s3:profile_id(), emqx_s3:profile_config()) -> supervisor:child_spec().
child_spec(ProfileId, ProfileConfig) ->
    #{
        id => ProfileId,
        start => {?MODULE, start_link, [ProfileId, ProfileConfig]},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [?MODULE]
    }.

%%--------------------------------------------------------------------
%% supervisor callbacks
%%-------------------------------------------------------------------

init([ProfileId, ProfileConfig]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    ChildSpecs = [
        %% Order matters
        emqx_s3_profile_conf:child_spec(ProfileId, ProfileConfig),
        emqx_s3_profile_uploader_sup:child_spec(ProfileId)
    ],
    {ok, {SupFlags, ChildSpecs}}.
