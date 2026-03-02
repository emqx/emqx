%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_schema).

%% API
-export([]).

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

namespace() -> a2a_registry.

roots() ->
    [
        {a2a_registry, mk(ref(a2a_registry), #{})}
    ].

fields(a2a_registry) ->
    [
        {enable, mk(boolean(), #{default => false, desc => ?DESC("enable")})},
        {validate_schema, mk(boolean(), #{default => true, desc => ?DESC("validate_schema")})}
    ].

desc(a2a_registry) ->
    ?DESC("a2a_registry");
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
