%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_examples).

%% Should return examples for the connector HTTP API. One can use the helper
%% function emqx_connector_schema:connector_values/3 to generate these
%% examples. See emqx_bridge_oracle:connector_examples/1 for an example.
-callback connector_examples(Method :: get | post | put) -> [map()].
