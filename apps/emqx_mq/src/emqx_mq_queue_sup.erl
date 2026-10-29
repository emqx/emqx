%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_queue_sup).
-behaviour(supervisor).

-include("emqx_mq_internal.hrl").

-export([start_link/1, child_spec/2, consumer/1, init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(MQ) ->
    supervisor:start_link(?MODULE, MQ).

child_spec(Id, Args) ->
    #{
        id => Id,
        start => {?MODULE, start_link, Args},
        restart => temporary,
        shutdown => infinity,
        type => supervisor
    }.

consumer(Sup) ->
    case lists:keyfind(?MQ_CONSUMER_CHILD_ID, 1, supervisor:which_children(Sup)) of
        {?MQ_CONSUMER_CHILD_ID, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
        _ -> {error, already_registered}
    end.

%%------------------------------------------------------------------------------
%% Supervisor callbacks
%%------------------------------------------------------------------------------

init(MQ) ->
    Flags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 10,
        auto_shutdown => any_significant
    },
    Consumer = maps:merge(emqx_mq_consumer:child_spec(MQ), #{
        restart => temporary, significant => true
    }),
    Cleaner =
        case emqx_mq_prop:on_full(MQ) of
            evict ->
                [];
            reject ->
                [emqx_mq_cleanup_worker:child_spec(MQ)]
        end,
    {ok, {Flags, [Consumer | Cleaner]}}.
