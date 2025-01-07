%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_schema).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    injected_fields/0
]).

namespace() -> emqx_shared_subs.

roots() ->
    [
        durable_queues
    ].

fields(durable_queues) ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    required => false,
                    default => false,
                    desc => ?DESC(enable)
                }
            )},
        duration(session_find_leader_timeout, 1000),
        duration(session_ping_leader_interval, 4000),
        duration(session_ping_leader_timeout, 4000),
        duration(session_unsubscribe_timeout, 1000),

        duration(leader_periodical_actions_interval, 1000),
        duration(leader_renew_streams_interval, 1000),
        duration(leader_borrower_timeout, 5000)
    ].

injected_fields() ->
    #{
        'durable_storage' => [
            {queues,
                emqx_ds_schema:db_schema(
                    basic,
                    #{
                        importance => ?IMPORTANCE_HIDDEN,
                        desc => ?DESC(durable_queues_storage)
                    }
                )}
        ]
    }.

duration(MsFieldName, Default) ->
    duration(MsFieldName, Default, ?IMPORTANCE_HIDDEN).

duration(MsFieldName, Default, Importance) ->
    {MsFieldName,
        ?HOCON(
            emqx_schema:timeout_duration_ms(),
            #{
                required => false,
                default => Default,
                desc => ?DESC(MsFieldName),
                importance => Importance
            }
        )}.

desc(durable_queues) -> "Settings for durable queues".
