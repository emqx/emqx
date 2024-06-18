%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_leader/1,
    stop_leader/1
]).

%% supervisor behaviour callbacks
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_leader(emqx_ds_shared_sub_leader:options()) -> supervisor:startchild_ret().
start_leader(Options) ->
    ChildSpec = emqx_ds_shared_sub_leader:child_spec(Options),
    supervisor:start_child(?MODULE, ChildSpec).

-spec stop_leader(emqx_ds_shared_sub_leader:topic_filter()) -> ok | {error, term()}.
stop_leader(TopicFilter) ->
    supervisor:terminate_child(?MODULE, emqx_ds_shared_sub_leader:id(TopicFilter)).

%%------------------------------------------------------------------------------
%% supervisor behaviour callbacks
%%------------------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
