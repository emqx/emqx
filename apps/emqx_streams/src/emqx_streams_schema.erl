%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() ->
    durable_streams.

roots() ->
    [durable_streams].

tags() ->
    [<<"Durable Streams">>].

fields(durable_streams) ->
    [
        {enable,
            mk(boolean(), #{
                default => true,
                desc => ?DESC(enable)
            })}
    ].

desc(durable_streams) ->
    ?DESC(durable_streams).

%%

mk(Type, Meta) ->
    hoconsc:mk(Type, Meta).
