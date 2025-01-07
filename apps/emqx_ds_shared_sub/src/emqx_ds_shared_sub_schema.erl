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
        duration(session_find_leader_timeout_ms, 1000),
        duration(session_renew_lease_timeout_ms, 5000),
        duration(session_min_update_stream_state_interval_ms, 500),

        duration(leader_renew_lease_interval_ms, 1000),
        duration(leader_renew_streams_interval_ms, 1000),
        duration(leader_drop_timeout_interval_ms, 1000),
        duration(leader_session_update_timeout_ms, 5000),
        duration(leader_session_not_replaying_timeout_ms, 5000)
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
    {MsFieldName,
        ?HOCON(
            emqx_schema:timeout_duration_ms(),
            #{
                required => false,
                default => Default,
                desc => ?DESC(MsFieldName),
                importance => ?IMPORTANCE_HIDDEN
            }
        )}.

desc(durable_queues) -> "Settings for durable queues".
