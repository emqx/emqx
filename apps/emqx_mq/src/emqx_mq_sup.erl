%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    start_consumer_sup/0,
    start_consumer/2
]).

-export([init/1]).

-define(ROOT_SUP, ?MODULE).
-define(CONSUMER_SUP, emqx_mq_consumer_sup).

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

start_consumer_sup() ->
    supervisor:start_link({local, ?CONSUMER_SUP}, ?MODULE, ?CONSUMER_SUP).

start_consumer(Id, Args) ->
    case supervisor:start_child(?CONSUMER_SUP, consumer_child_spec(Id, Args)) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

init(?ROOT_SUP) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [message_db_watcher_child_spec(), consumer_sup_child_spec()],
    {ok, {SupFlags, ChildSpecs}};
init(?CONSUMER_SUP) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

consumer_sup_child_spec() ->
    #{
        id => ?CONSUMER_SUP,
        start => {?MODULE, start_consumer_sup, []},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [?MODULE]
    }.

consumer_child_spec(Id, Args) ->
    #{
        id => Id,
        start => {emqx_mq_consumer, start_link, Args},
        restart => temporary,
        shutdown => 5000
    }.

message_db_watcher_child_spec() ->
    #{
        id => emqx_mq_message_db_watcher,
        start => {emqx_mq_message_db_watcher, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.
