%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_ee_schema).

-if(?EMQX_RELEASE_EDITION == ee).

-export([
    resource_type/1,
    connector_impl_module/1
]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    api_schemas/1,
    fields/1,
    schema_modules/0,
    namespace/0
]).

resource_type(Type) when is_binary(Type) ->
    resource_type(binary_to_atom(Type, utf8));
resource_type(Type) ->
    error({unknown_connector_type, Type}).

%% For connectors that need to override connector configurations.
connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(_ConnectorType) ->
    undefined.

namespace() -> undefined.

fields(connectors) ->
    connector_structs().

connector_structs() ->
    [].

schema_modules() ->
    [].

api_schemas(_Method) ->
    [].

-else.

-endif.
