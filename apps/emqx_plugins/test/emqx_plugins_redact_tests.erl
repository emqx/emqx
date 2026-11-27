%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_redact_tests).

-include_lib("eunit/include/eunit.hrl").

-define(PLUGIN, <<"some_plugin-1.0.0">>).
-define(MASK, <<"******">>).

redact(Config) -> emqx_plugins_redact:redact(?PLUGIN, Config).
restore(New, Old) -> emqx_plugins_redact:deobfuscate(?PLUGIN, New, Old).

roundtrip(Stored, Expected) ->
    Redacted = redact(Stored),
    [
        ?_assertEqual(Expected, Redacted),
        ?_assertEqual(Stored, restore(Redacted, Stored))
    ].

string_test_() ->
    roundtrip(
        #{<<"host">> => <<"h">>, <<"password">> => <<"p">>},
        #{<<"host">> => <<"h">>, <<"password">> => ?MASK}
    ).

union_test_() ->
    roundtrip(
        #{<<"auth">> => #{<<"password">> => #{<<"string">> => <<"p">>}}},
        #{<<"auth">> => #{<<"password">> => #{<<"string">> => ?MASK}}}
    ).

-doc "A value that is not a string keeps its Avro type and is not masked.".
other_types_test_() ->
    Stored = #{
        <<"password">> => null,
        <<"token">> => 7,
        <<"secret">> => true,
        <<"api_key">> => [<<"a">>]
    },
    roundtrip(Stored, Stored).

-doc "A record under a sensitive name is not replaced. Its fields follow the same rules.".
record_test_() ->
    roundtrip(
        #{<<"secret">> => #{<<"password">> => <<"p">>, <<"user">> => <<"u">>}},
        #{<<"secret">> => #{<<"password">> => ?MASK, <<"user">> => <<"u">>}}
    ).

file_path_and_placeholder_test_() ->
    Stored = #{<<"password">> => <<"file:///etc/pw">>, <<"token">> => <<"${token}">>},
    roundtrip(Stored, Stored).

header_list_test_() ->
    roundtrip(
        #{
            <<"headers">> => [
                #{<<"name">> => <<"Authorization">>, <<"value">> => <<"s1">>},
                #{<<"name">> => <<"X-Label">>, <<"value">> => <<"v">>}
            ]
        },
        #{
            <<"headers">> => [
                #{<<"name">> => <<"Authorization">>, <<"value">> => ?MASK},
                #{<<"name">> => <<"X-Label">>, <<"value">> => <<"v">>}
            ]
        }
    ).

-doc "Name and value entries outside a headers field are not masked.".
other_entries_test_() ->
    Stored = #{
        <<"args">> => [
            #{<<"name">> => <<"password">>, <<"value">> => <<"p">>},
            #{<<"key">> => <<"Authorization">>, <<"value">> => <<"a">>}
        ]
    },
    roundtrip(Stored, Stored).

entry_restore_test_() ->
    Single = #{<<"headers">> => [#{<<"name">> => <<"Authorization">>, <<"value">> => <<"s">>}]},
    Duplicates = #{
        <<"headers">> => [
            #{<<"name">> => <<"Authorization">>, <<"value">> => <<"a">>},
            #{<<"name">> => <<"authorization">>, <<"value">> => <<"b">>}
        ]
    },
    [
        %% Reordered entries are matched by name.
        ?_assertEqual(
            #{
                <<"headers">> => [
                    #{<<"name">> => <<"X">>, <<"value">> => <<"v">>},
                    #{<<"name">> => <<"Authorization">>, <<"value">> => <<"s">>}
                ]
            },
            restore(
                #{
                    <<"headers">> => [
                        #{<<"name">> => <<"X">>, <<"value">> => <<"v">>},
                        #{<<"name">> => <<"Authorization">>, <<"value">> => ?MASK}
                    ]
                },
                Single
            )
        ),
        %% Names are matched case-sensitively.
        ?_assertEqual(
            #{<<"headers">> => []},
            restore(
                #{<<"headers">> => [#{<<"name">> => <<"authorization">>, <<"value">> => ?MASK}]},
                Single
            )
        ),
        ?_assertEqual(
            #{<<"headers">> => []},
            restore(
                #{<<"headers">> => [#{<<"name">> => <<"AUTHORIZATION">>, <<"value">> => ?MASK}]},
                Duplicates
            )
        ),
        %% Both duplicates keep their own values.
        ?_assertEqual(Duplicates, restore(redact(Duplicates), Duplicates)),
        %% No stored entry.
        ?_assertEqual(
            #{<<"headers">> => []},
            restore(
                #{<<"headers">> => [#{<<"name">> => <<"Authorization">>, <<"value">> => ?MASK}]},
                #{}
            )
        ),
        %% A new value replaces the stored one. A mask in a non-sensitive entry is a value.
        ?_assertEqual(
            #{
                <<"headers">> => [
                    #{<<"name">> => <<"Authorization">>, <<"value">> => <<"new">>},
                    #{<<"name">> => <<"X">>, <<"value">> => ?MASK}
                ]
            },
            restore(
                #{
                    <<"headers">> => [
                        #{<<"name">> => <<"Authorization">>, <<"value">> => <<"new">>},
                        #{<<"name">> => <<"X">>, <<"value">> => ?MASK}
                    ]
                },
                Single
            )
        )
    ].

header_map_test_() ->
    roundtrip(
        #{<<"headers">> => #{<<"Authorization">> => <<"s">>, <<"X">> => <<"v">>}},
        #{<<"headers">> => #{<<"Authorization">> => ?MASK, <<"X">> => <<"v">>}}
    ).

-doc "A masked value without a stored value is dropped.".
missing_stored_value_test() ->
    ?assertEqual(
        #{<<"auth">> => #{<<"user">> => <<"u">>}},
        restore(
            #{<<"auth">> => #{<<"password">> => #{<<"string">> => ?MASK}, <<"user">> => <<"u">>}},
            #{}
        )
    ).

-doc "List entries without a name, an id or a key are not matched, so their masked values are dropped.".
unmatched_list_test() ->
    Stored = #{<<"items">> => [#{<<"label">> => <<"a">>, <<"password">> => <<"p">>}]},
    Redacted = redact(Stored),
    ?assertEqual(#{<<"items">> => [#{<<"label">> => <<"a">>, <<"password">> => ?MASK}]}, Redacted),
    ?assertEqual(#{<<"items">> => [#{<<"label">> => <<"a">>}]}, restore(Redacted, Stored)).

%%--------------------------------------------------------------------
%% Records in lists, matched by id and type
%%--------------------------------------------------------------------

-doc """
List entries are matched by `id` and `type`. Two entries here share the id `x` and differ by
type. Their records are wrapped in a union record name, as in Avro JSON.
""".
records_test_() ->
    Stored = #{
        <<"items">> => [
            #{
                <<"Pg">> => #{
                    <<"type">> => <<"pg">>, <<"id">> => <<"x">>, <<"password">> => <<"pa">>
                }
            },
            #{
                <<"My">> => #{
                    <<"type">> => <<"my">>, <<"id">> => <<"x">>, <<"password">> => <<"pb">>
                }
            },
            #{<<"id">> => <<"c">>, <<"password">> => <<"pc">>}
        ]
    },
    Masked = #{
        <<"items">> => [
            #{<<"Pg">> => #{<<"type">> => <<"pg">>, <<"id">> => <<"x">>, <<"password">> => ?MASK}},
            #{<<"My">> => #{<<"type">> => <<"my">>, <<"id">> => <<"x">>, <<"password">> => ?MASK}},
            #{<<"id">> => <<"c">>, <<"password">> => ?MASK}
        ]
    },
    [
        ?_assertEqual(Masked, redact(Stored)),
        ?_assertEqual(Stored, restore(Masked, Stored)),
        %% Entries are matched by id and type, not by position.
        ?_assertEqual(
            #{
                <<"items">> => [
                    #{<<"id">> => <<"c">>, <<"password">> => <<"pc">>},
                    #{
                        <<"My">> => #{
                            <<"type">> => <<"my">>, <<"id">> => <<"x">>, <<"password">> => <<"pb">>
                        }
                    },
                    #{
                        <<"Pg">> => #{
                            <<"type">> => <<"pg">>, <<"id">> => <<"x">>, <<"password">> => <<"pa">>
                        }
                    }
                ]
            },
            restore(
                #{
                    <<"items">> => [
                        #{<<"id">> => <<"c">>, <<"password">> => ?MASK},
                        #{
                            <<"My">> => #{
                                <<"type">> => <<"my">>, <<"id">> => <<"x">>, <<"password">> => ?MASK
                            }
                        },
                        #{
                            <<"Pg">> => #{
                                <<"type">> => <<"pg">>, <<"id">> => <<"x">>, <<"password">> => ?MASK
                            }
                        }
                    ]
                },
                Stored
            )
        ),
        %% A new entry has no stored value, so its masked password is dropped.
        ?_assertEqual(
            #{<<"items">> => [#{<<"id">> => <<"new">>}]},
            restore(#{<<"items">> => [#{<<"id">> => <<"new">>, <<"password">> => ?MASK}]}, Stored)
        )
    ].

-doc "List entries with only an `id` field are matched by `id`.".
records_id_only_test() ->
    Stored = #{
        <<"items">> => [
            #{<<"id">> => <<"a">>, <<"password">> => <<"pa">>},
            #{<<"id">> => <<"b">>, <<"password">> => <<"pb">>}
        ]
    },
    ?assertEqual(
        #{
            <<"items">> => [
                #{<<"id">> => <<"b">>, <<"password">> => <<"pb">>},
                #{<<"id">> => <<"a">>, <<"password">> => <<"pa">>}
            ]
        },
        restore(
            #{
                <<"items">> => [
                    #{<<"id">> => <<"b">>, <<"password">> => ?MASK},
                    #{<<"id">> => <<"a">>, <<"password">> => ?MASK}
                ]
            },
            Stored
        )
    ).

-doc """
List entries with only a `type` field are not matched, also when a stored entry has the same
`type`, so their masked values are dropped.
""".
records_type_only_test() ->
    Stored = #{<<"items">> => [#{<<"type">> => <<"pg">>, <<"password">> => <<"pa">>}]},
    ?assertEqual(
        #{<<"items">> => [#{<<"type">> => <<"pg">>}]},
        restore(#{<<"items">> => [#{<<"type">> => <<"pg">>, <<"password">> => ?MASK}]}, Stored)
    ).
