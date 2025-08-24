%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dsch_proto_v1).

-behavior(emqx_bpapi).

%% API:
-export([get_site_schemas/2]).

%% behavior callbacks:
-export([introduced_in/0]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("emqx_utils/include/bpapi.hrl").
-include("../emqx_dsch.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-spec get_site_schemas([node()], timeout()) ->
    [{ok, emqx_dsch:schema() | ?empty_schema} | _Err].
get_site_schemas(Nodes, Timeout) ->
    erpc:multicall(Nodes, emqx_dsch, get_site_schema, [], Timeout).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "6.0.0".
