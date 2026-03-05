%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Converts a JSON Schema properties map into a full object schema.
%% All keys present in Props are declared as required.

-module(emqx_agent_skill_schema).

-export([to_schema/1]).

-type props() :: #{binary() => map()}.

%% Wraps a properties map into #{type => object, properties => ..., required => [all keys]}.
-spec to_schema(props()) -> map().
to_schema(Props) when is_map(Props) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => Props,
        <<"required">> => maps:keys(Props)
    }.
