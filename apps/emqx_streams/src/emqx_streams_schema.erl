%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_schema).

-include("emqx_streams_internal.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() ->
    ?SCHEMA_ROOT.

roots() ->
    [?SCHEMA_ROOT].

tags() ->
    [<<"Durable Streams">>].

fields(?SCHEMA_ROOT) ->
    [
        {enable,
            mk(boolean(), #{
                default => true,
                desc => ?DESC(enable)
            })}
    ].

desc(?SCHEMA_ROOT) ->
    ?DESC(streams).

%%

mk(Type, Meta) ->
    hoconsc:mk(Type, Meta).
