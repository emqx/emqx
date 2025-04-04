%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% JSON lines container implementation for `emqx_connector_aggregator`.
-module(emqx_connector_aggreg_json_lines).

-behaviour(emqx_connector_aggreg_container).

%% `emqx_connector_aggreg_container' API
-export([
    new/1,
    fill/2,
    close/1
]).

-export_type([container/0, options/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-record(jsonl, {}).

-opaque container() :: #jsonl{}.

-type options() :: #{}.

-type write_metadata() :: emqx_connector_aggreg_container:write_metadata().

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_container' API
%%------------------------------------------------------------------------------

-spec new(options()) -> container().
new(_Opts) ->
    #jsonl{}.

-spec fill([emqx_connector_aggregator:record()], container()) ->
    {iodata(), write_metadata(), container()}.
fill(Records, JSONL) ->
    Output = lists:map(fun(Record) -> [emqx_utils_json:encode(Record), $\n] end, Records),
    {Output, #{}, JSONL}.

-spec close(container()) -> {iodata(), write_metadata()}.
close(#jsonl{}) ->
    {[], #{}}.
