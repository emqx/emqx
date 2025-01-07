%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ext_schema).
-behaviour(emqx_schema_hooks).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% emqx_schema_hooks callbacks
%%------------------------------------------------------------------------------
-export([injected_fields/0]).

-spec injected_fields() -> #{emqx_schema_hooks:hookpoint() => [hocon_schema:field()]}.
injected_fields() ->
    #{
        'common_ssl_opts_schema' => fields(auth_ext)
    }.

fields(auth_ext) ->
    [
        {"partial_chain",
            sc(
                hoconsc:enum([true, false, two_cacerts_from_cacertfile, cacert_from_cacertfile]),
                #{
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_partial_chain)
                }
            )},
        {"verify_peer_ext_key_usage",
            sc(
                string(),
                #{
                    required => false,
                    desc => ?DESC(common_ssl_opts_verify_peer_ext_key_usage)
                }
            )}
    ].

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
