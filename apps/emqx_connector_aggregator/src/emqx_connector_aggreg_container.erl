%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggreg_container).

-export([new/2, fill/3, close/2]).

-export_type([container/0, new_opts/0]).

%%-----------------------------------------------------------------------------
%% Type declarations
%%-----------------------------------------------------------------------------

-type container() :: term().

-type new_opts() :: map().

%%-----------------------------------------------------------------------------
%% Callbacks
%%-----------------------------------------------------------------------------

-callback new(new_opts()) -> container().

-callback fill([emqx_connector_aggregator:record()], Container) ->
    {iodata() | term(), Container}
when
    Container :: container().

-callback close(container()) -> iodata() | term().

%%-----------------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------------

-spec new(module(), new_opts()) -> container().
new(Mod, Opts) ->
    Mod:new(Opts).

-spec fill(module(), [emqx_connector_aggregator:record()], Container) ->
    {iodata() | term(), Container}
when
    Container :: container().
fill(Mod, Records, Container) ->
    Mod:fill(Records, Container).

-spec close(module(), container()) -> iodata() | term().
close(Mod, Container) ->
    Mod:close(Container).
