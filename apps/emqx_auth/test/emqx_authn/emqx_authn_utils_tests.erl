%%--------------------------------------------------------------------
%% Copyright (c) 2026-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_utils_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ATTRS(SELECTED), maps:get(client_attrs, emqx_authn_utils:client_attrs(SELECTED))).

client_attrs_shapes_test_() ->
    [
        {"a whole map, as an HTTP body or a Mongo subdocument returns it", fun() ->
            ?assertEqual(
                #{<<"sn">> => <<"s1">>},
                ?ATTRS(#{<<"client_attrs">> => #{<<"sn">> => <<"s1">>}})
            )
        end},
        {"one key per attribute, as a SQL column alias produces", fun() ->
            ?assertEqual(
                #{<<"sn">> => <<"s1">>, <<"tier">> => <<"gold">>},
                ?ATTRS(#{
                    <<"password_hash">> => <<"h">>,
                    <<"client_attrs.sn">> => <<"s1">>,
                    <<"client_attrs.tier">> => <<"gold">>
                })
            )
        end},
        {"a per-attribute key wins over the same name in the map", fun() ->
            ?assertEqual(
                #{<<"sn">> => <<"from_column">>, <<"tier">> => <<"gold">>},
                ?ATTRS(#{
                    <<"client_attrs">> => #{
                        <<"sn">> => <<"from_map">>, <<"tier">> => <<"gold">>
                    },
                    <<"client_attrs.sn">> => <<"from_column">>
                })
            )
        end},
        {"no attributes at all", fun() ->
            ?assertEqual(#{}, ?ATTRS(#{<<"password_hash">> => <<"h">>}))
        end},
        {"the prefix alone is not an attribute name", fun() ->
            ?assertEqual(#{}, ?ATTRS(#{<<"client_attrs.">> => <<"v">>}))
        end},
        {"a non-map client_attrs is ignored rather than crashing", fun() ->
            ?assertEqual(#{}, ?ATTRS(#{<<"client_attrs">> => <<"{\"sn\":\"s1\"}">>}))
        end},
        {"a non-map selected value is ignored", fun() ->
            ?assertEqual(#{}, ?ATTRS(not_a_map))
        end}
    ].

%% Database columns are typed, and `iolist_to_binary/1' raises badarg on an
%% integer, which emqx_authn_chains turns into an authenticator_error -- the
%% whole login fails over one attribute.
client_attrs_value_types_test_() ->
    [
        {"integers, floats and booleans are usable attribute values", fun() ->
            ?assertEqual(
                #{
                    <<"n">> => <<"42">>,
                    <<"f">> => <<"1.5">>,
                    <<"yes">> => <<"true">>,
                    <<"no">> => <<"false">>
                },
                ?ATTRS(#{
                    <<"client_attrs.n">> => 42,
                    <<"client_attrs.f">> => 1.5,
                    <<"client_attrs.yes">> => true,
                    <<"client_attrs.no">> => false
                })
            )
        end},
        {"a list value is dropped rather than flattened", fun() ->
            %% Backends return text as a binary. A list is more likely a
            %% driver returning a column as a list of integers, which
            %% `iolist_to_binary/1' would render as arbitrary bytes.
            ?assertEqual(
                #{},
                ?ATTRS(#{<<"client_attrs.sn">> => ["a", <<"b">>, $c]})
            ),
            ?assertEqual(
                #{},
                ?ATTRS(#{<<"client_attrs.sn">> => [103, 111, 108, 100]})
            )
        end},
        {"a NULL column is dropped, not turned into a string", fun() ->
            ?assertEqual(
                #{<<"kept">> => <<"v">>},
                ?ATTRS(#{
                    <<"client_attrs.missing">> => undefined,
                    <<"client_attrs.null">> => null,
                    <<"client_attrs.kept">> => <<"v">>
                })
            )
        end},
        {"a value with no binary representation is dropped", fun() ->
            ?assertEqual(
                #{},
                ?ATTRS(#{<<"client_attrs.nested">> => #{<<"a">> => <<"b">>}})
            )
        end}
    ].

%% A backend result keeps its previous shape when there is nothing to report,
%% so adding attribute support to a backend does not change every other result.
maybe_client_attrs_test_() ->
    [
        {"the key is left out when there are no attributes", fun() ->
            ?assertEqual(
                #{},
                emqx_authn_utils:maybe_client_attrs(#{<<"password_hash">> => <<"h">>})
            )
        end},
        {"the key is left out when every attribute was dropped", fun() ->
            ?assertEqual(
                #{},
                emqx_authn_utils:maybe_client_attrs(#{<<"client_attrs.sn">> => undefined})
            )
        end},
        {"the key is present when there is something to report", fun() ->
            ?assertEqual(
                #{client_attrs => #{<<"sn">> => <<"s1">>}},
                emqx_authn_utils:maybe_client_attrs(#{<<"client_attrs.sn">> => <<"s1">>})
            )
        end}
    ].

client_attrs_names_test_() ->
    [
        {"an attribute name outside the allowed character set is dropped", fun() ->
            ?assertEqual(
                #{<<"ok_name">> => <<"v">>},
                ?ATTRS(#{
                    <<"client_attrs.ok_name">> => <<"v">>,
                    <<"client_attrs.bad name">> => <<"v">>,
                    <<"client_attrs.-leading">> => <<"v">>
                })
            )
        end}
    ].
