%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_listeners_tests).

-include_lib("eunit/include/eunit.hrl").

reconcile_cert_source_test_() ->
    Managed = [#{<<"bundle_name">> => <<"bundle">>}],
    Merged = #{<<"ssl_options">> => #{<<"managed_certs">> => Managed}},
    Cleared = #{<<"ssl_options">> => #{<<"managed_certs">> => null}},
    Reconcile = fun(Request) -> emqx_mgmt_api_listeners:reconcile_cert_source(Request, Merged) end,
    ReqSSL = fun(SSL) -> #{<<"ssl_options">> => SSL} end,
    [
        %% Switching to file certificates, or clearing outright, marks the merged
        %% config so the stale bundle does not survive the merge in
        %% `emqx_listeners:pre_config_update/3'.
        {"a certfile switches the source",
            ?_assertEqual(Cleared, Reconcile(ReqSSL(#{<<"certfile">> => <<"c.pem">>})))},
        {"a keyfile switches the source",
            ?_assertEqual(Cleared, Reconcile(ReqSSL(#{<<"keyfile">> => <<"k.pem">>})))},
        {"null clears", ?_assertEqual(Cleared, Reconcile(ReqSSL(#{<<"managed_certs">> => null})))},
        %% A request can carry an empty string: `hocon' passes it through the
        %% schema check unaltered, so normalising it to `null' here is what keeps
        %% it from reaching the listener as a bundle reference.
        {"an empty string clears",
            ?_assertEqual(Cleared, Reconcile(ReqSSL(#{<<"managed_certs">> => <<>>})))},
        %% The schema validator rejects an empty array before the handler sees
        %% it, so this only covers a direct caller.
        {"an empty array clears",
            ?_assertEqual(Cleared, Reconcile(ReqSSL(#{<<"managed_certs">> => []})))},
        %% A request that names a bundle, or no certificate at all, is left alone:
        %% the latter is indistinguishable from a partial update of other fields.
        {"a bundle is kept",
            ?_assertEqual(Merged, Reconcile(ReqSSL(#{<<"managed_certs">> => Managed})))},
        %% The field is a union: a single bundle object is as valid as an array
        %% of them, and means the same thing here.
        {"a single bundle object is kept",
            ?_assertEqual(
                Merged, Reconcile(ReqSSL(#{<<"managed_certs">> => hd(Managed)}))
            )},
        %% A bundle named alongside file certificates still wins: naming a
        %% bundle is not a switch away from one.
        {"a bundle named with file certificates is kept",
            ?_assertEqual(
                Merged,
                Reconcile(
                    ReqSSL(#{
                        <<"managed_certs">> => Managed, <<"certfile">> => <<"c.pem">>
                    })
                )
            )},
        {"an unrelated ssl option is kept",
            ?_assertEqual(Merged, Reconcile(ReqSSL(#{<<"verify">> => <<"verify_none">>})))},
        {"a request without ssl_options is kept",
            ?_assertEqual(Merged, Reconcile(#{<<"bind">> => <<"0.0.0.0:8883">>}))}
    ].
