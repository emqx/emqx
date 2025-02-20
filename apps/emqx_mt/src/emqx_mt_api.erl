%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include("emqx_mt.hrl").
%% -include_lib("emqx/include/logger.hrl").

%% `minirest' and `minirest_trails' API
-export([
    namespace/0,
    api_spec/0,
    fields/1,
    paths/0,
    schema/1
]).

%% `minirest' handlers
-export([
    ns_list/2,
    client_list/2,
    client_count/2
]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-define(TAGS, [<<"Multi-tenancy">>]).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "mt".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/mt/ns_list",
        "/mt/ns/:ns/client_list",
        "/mt/ns/:ns/client_count"
    ].

schema("/mt/ns_list") ->
    #{
        'operationId' => ns_list,
        get => #{
            tags => ?TAGS,
            summary => <<"List Namespaces">>,
            description => ?DESC("ns_list"),
            parameters => [
                last_ns_in_query(),
                limit_in_query()
            ],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(binary()),
                            example_ns_list()
                        )
                }
        }
    };
schema("/mt/ns/:ns/client_list") ->
    #{
        'operationId' => client_list,
        get => #{
            tags => ?TAGS,
            summary => <<"List Clients in a Namespace">>,
            description => ?DESC("client_list"),
            parameters => [
                param_path_ns(),
                last_clientid_in_query(),
                limit_in_query()
            ],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(binary()),
                            example_client_list()
                        ),
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    };
schema("/mt/ns/:ns/client_count") ->
    #{
        'operationId' => client_count,
        get => #{
            tags => ?TAGS,
            summary => <<"Count Clients in a Namespace">>,
            description => ?DESC("client_count"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    200 => [{count, mk(non_neg_integer(), #{desc => <<"Client count">>})}],
                    404 => error_schema('NOT_FOUND', "Namespace not found")
                }
        }
    }.

param_path_ns() ->
    {ns,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"ns1">>,
                desc => ?DESC("param_path_ns")
            }
        )}.

last_ns_in_query() ->
    {last_ns,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"ns1">>,
                desc => ?DESC("last_ns_in_query")
            }
        )}.

limit_in_query() ->
    {limit,
        mk(
            pos_integer(),
            #{
                in => query,
                required => false,
                example => 100,
                desc => ?DESC("limit_in_query")
            }
        )}.

last_clientid_in_query() ->
    {last_clientid,
        mk(
            binary(),
            #{
                in => query,
                required => false,
                example => <<"clientid1">>,
                desc => ?DESC("last_clientid_in_query")
            }
        )}.

%% no structs in this schema
fields(_) -> [].

mk(Type, Props) -> hoconsc:mk(Type, Props).

array(Type) -> hoconsc:array(Type).

error_schema(Code, Message) ->
    BinMsg = unicode:characters_to_binary(Message),
    emqx_dashboard_swagger:error_codes([Code], BinMsg).

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

ns_list(get, Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastNs = maps:get(<<"last_ns">>, QS, ?MIN_NS),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    ?OK(emqx_mt:list_ns(LastNs, Limit)).

client_list(get, #{bindings := #{ns := Ns}} = Params) ->
    QS = maps:get(query_string, Params, #{}),
    LastClientId = maps:get(<<"last_clientid">>, QS, ?MIN_CLIENTID),
    Limit = maps:get(<<"limit">>, QS, ?DEFAULT_PAGE_SIZE),
    case emqx_mt:list_clients(Ns, LastClientId, Limit) of
        {ok, Clients} -> ?OK(Clients);
        {error, not_found} -> ?NOT_FOUND("Namespace not found")
    end.

client_count(get, #{bindings := #{ns := Ns}}) ->
    case emqx_mt:count_clients(Ns) of
        {ok, Count} -> ?OK(#{count => Count});
        {error, not_found} -> ?NOT_FOUND("Namespace not found")
    end.

%%-------------------------------------------------------------------------------------------------
%% helper functions
%%-------------------------------------------------------------------------------------------------
example_ns_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => [<<"ns1">>, <<"ns2">>]
            }
    }.

example_client_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => [<<"client1">>, <<"client2">>]
            }
    }.
