%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_action_enterprise).

-if(?EMQX_RELEASE_EDITION == ee).

-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    fields/1
]).

fields(bridges_v2) ->
    kafka_structs().

kafka_structs() ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, kafka_producer_action)),
                #{
                    desc => <<"Kafka Producer Bridge V2 Config">>,
                    required => false
                }
            )}
    ].

-else.

-endif.
