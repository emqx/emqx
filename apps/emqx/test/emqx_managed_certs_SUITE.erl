%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_managed_certs_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start([emqx_conf], #{work_dir => emqx_cth_suite:work_dir(TCConfig)}),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

mk_managed_certs_struct(?global_ns, Bundle) ->
    #{<<"bundle_name">> => Bundle};
mk_managed_certs_struct(Ns, Bundle) when is_binary(Ns) ->
    #{<<"namespace">> => Ns, <<"bundle_name">> => Bundle}.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_find_references(_TCConfig) ->
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,
    Mk = fun(Ns, Bundle) -> mk_managed_certs_struct(Ns, Bundle) end,
    NsConfigs = #{
        ?global_ns => #{
            <<"a0">> => #{
                <<"a1">> => [
                    #{<<"managed_certs">> => Mk(?global_ns, <<"bundle1">>)},
                    [#{<<"managed_certs">> => Mk(?global_ns, <<"bundle2">>)}],
                    #{<<"ssl">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle3">>)}}
                ],
                <<"a2">> => #{
                    <<"a3">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle4">>)},
                    <<"a4">> => [#{<<"managed_certs">> => Mk(?global_ns, <<"bundle5">>)}],
                    <<"a5">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle4">>)}
                }
            },
            <<"b0">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle6">>)},
            <<"c0">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle1">>)},
            <<"d0">> => #{
                <<"ssl_options">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle7">>)}
            }
        },
        Ns1 => #{
            <<"a0">> => #{
                <<"a1">> => [
                    #{<<"managed_certs">> => Mk(Ns1, <<"bundle1">>)},
                    [#{<<"managed_certs">> => Mk(Ns1, <<"bundle2">>)}],
                    #{<<"ssl">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle3">>)}}
                ],
                <<"a2">> => #{
                    <<"a3">> => #{<<"managed_certs">> => Mk(Ns2, <<"bundle4">>)},
                    <<"a4">> => [#{<<"managed_certs">> => Mk(?global_ns, <<"bundle5">>)}],
                    <<"a5">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle4">>)},
                    <<"a6">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle1">>)}
                }
            }
        }
    },
    Refs = fun(Ns, Bundle) ->
        emqx_managed_certs:do_find_references(NsConfigs, Ns, Bundle)
    end,

    ?assertSameSet([], Refs(Ns2, <<"bundle1">>)),

    %% Same bundle name exists on multiple namespaces.
    ?assertSameSet(
        [{?global_ns, [<<"a0">>, <<"a2">>, <<"a3">>]}],
        Refs(?global_ns, <<"bundle4">>)
    ),
    ?assertSameSet(
        [{Ns1, [<<"a0">>, <<"a2">>, <<"a3">>]}],
        Refs(Ns2, <<"bundle4">>)
    ),

    %% Same reference on different namespaces
    ?assertSameSet(
        [
            {?global_ns, [<<"c0">>]},
            {Ns1, [<<"a0">>, <<"a1">>, 1]},
            {Ns1, [<<"a0">>, <<"a2">>, <<"a6">>]}
        ],
        Refs(Ns1, <<"bundle1">>)
    ),
    ?assertSameSet(
        [
            {?global_ns, [<<"a0">>, <<"a2">>, <<"a4">>, 1]},
            {Ns1, [<<"a0">>, <<"a2">>, <<"a4">>, 1]}
        ],
        Refs(?global_ns, <<"bundle5">>)
    ),

    %% Pretty path
    ?assertSameSet(
        [{?global_ns, [<<"a0">>, <<"a1">>, 3]}],
        Refs(?global_ns, <<"bundle3">>)
    ),
    ?assertSameSet(
        [{Ns1, [<<"a0">>, <<"a1">>, 3]}],
        Refs(Ns1, <<"bundle3">>)
    ),
    ?assertSameSet(
        [{?global_ns, [<<"d0">>]}],
        Refs(?global_ns, <<"bundle7">>)
    ),

    ok.
