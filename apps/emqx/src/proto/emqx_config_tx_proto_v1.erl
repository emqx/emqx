%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_config. A replicant passes them to mria by
%% name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_config_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([save_configs_namespaced/5]).

introduced_in() ->
    "6.3.3".

-spec save_configs_namespaced(binary(), binary(), map(), map(), map()) -> ok.
save_configs_namespaced(Namespace, RootKeyBin, Conf, RawConf, Opts) ->
    emqx_config:save_configs_namespaced_tx(Namespace, RootKeyBin, Conf, RawConf, Opts).
