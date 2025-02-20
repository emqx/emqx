%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggregator_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([
    container/0,
    container/1
]).

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

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

container() ->
    container(_Opts = #{}).

container(Opts) ->
    AllTypes = [<<"csv">>, <<"json_lines">>],
    SupportedTypes = maps:get(supported_types, Opts, AllTypes),
    %% Assert
    true = SupportedTypes =/= [],
    Default = maps:get(default, Opts, <<"csv">>),
    %% Assert
    true = lists:member(Default, SupportedTypes),
    Selector0 = #{
        <<"csv">> => ref(container_csv),
        <<"json_lines">> => ref(container_json_lines)
    },
    Selector = maps:with(SupportedTypes, Selector0),
    MetaOverrides = maps:get(meta, Opts, #{}),
    {container,
        hoconsc:mk(
            emqx_schema:mkunion(type, Selector, Default),
            emqx_utils_maps:deep_merge(
                #{
                    required => true,
                    default => #{<<"type">> => Default},
                    desc => ?DESC("container")
                },
                MetaOverrides
            )
        )}.

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() -> "connector_aggregator".

roots() -> [].

fields(container_csv) ->
    [
        {type,
            mk(
                csv,
                #{
                    required => true,
                    desc => ?DESC("container_csv")
                }
            )},
        {column_order,
            mk(
                hoconsc:array(string()),
                #{
                    required => false,
                    default => [],
                    desc => ?DESC("container_csv_column_order")
                }
            )}
    ];
fields(container_json_lines) ->
    [
        {type,
            mk(
                json_lines,
                #{
                    required => true,
                    desc => ?DESC("container_json_lines")
                }
            )}
    ].

desc(Name) when
    Name == container_csv;
    Name == container_json_lines
->
    ?DESC(Name);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
