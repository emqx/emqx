%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements the config schema for emqx_mt app.
-module(emqx_mt_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1
]).

namespace() -> emqx_mt.

roots() ->
    [
        {multi_tenancy, mk(ref("config"), #{importance => ?IMPORTANCE_MEDIUM})}
    ].

fields("config") ->
    [
        {default_session_limit,
            mk(
                union(
                    [infinity, non_neg_integer()]
                ),
                #{
                    desc => ?DESC(default_session_limit),
                    importance => ?IMPORTANCE_HIGH,
                    default => infinity
                }
            )}
    ].

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
union(Types) -> hoconsc:union(Types).
