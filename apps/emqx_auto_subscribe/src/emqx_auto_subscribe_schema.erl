%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auto_subscribe_schema).

-behaviour(hocon_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

namespace() -> "auto_subscribe".

roots() ->
    [{"auto_subscribe", ?HOCON(?R_REF("auto_subscribe"), #{importance => ?IMPORTANCE_HIDDEN})}].

fields("auto_subscribe") ->
    [
        {topics,
            ?HOCON(
                ?ARRAY(?R_REF("topic")),
                #{
                    desc => ?DESC(auto_subscribe),
                    default => []
                }
            )}
    ];
fields("topic") ->
    [
        {topic,
            ?HOCON(binary(), #{
                required => true,
                example => topic_example(),
                desc => ?DESC("topic")
            })},
        {qos,
            ?HOCON(emqx_schema:qos(), #{
                default => 0,
                desc => ?DESC("qos")
            })},
        {rh,
            ?HOCON(range(0, 2), #{
                default => 0,
                desc => ?DESC("rh")
            })},
        {rap,
            ?HOCON(range(0, 1), #{
                default => 0,
                desc => ?DESC("rap")
            })},
        {nl,
            ?HOCON(range(0, 1), #{
                default => 0,
                desc => ?DESC(nl)
            })}
    ].

desc("auto_subscribe") -> ?DESC("auto_subscribe");
desc("topic") -> ?DESC("topic");
desc(_) -> undefined.

topic_example() ->
    <<"/clientid/", ?PH_S_CLIENTID, "/username/", ?PH_S_USERNAME, "/host/", ?PH_S_HOST, "/port/",
        ?PH_S_PORT>>.
