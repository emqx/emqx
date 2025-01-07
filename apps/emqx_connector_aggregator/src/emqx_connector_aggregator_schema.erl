%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggregator_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([
    container/0
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
    {container,
        hoconsc:mk(
            %% TODO: Support selectors once there are more than one container.
            hoconsc:union(fun
                (all_union_members) -> [ref(container_csv)];
                ({value, _Value}) -> [ref(container_csv)]
            end),
            #{
                required => true,
                default => #{<<"type">> => <<"csv">>},
                desc => ?DESC("container")
            }
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
    ].

desc(Name) when
    Name == container_csv
->
    ?DESC(Name);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
