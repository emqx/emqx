%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_mgmt_data_backup. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_mgmt_data_backup_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([restore_mnesia_tab/2]).

introduced_in() ->
    "6.3.3".

-spec restore_mnesia_tab(mria:table(), [tuple()]) -> ok.
restore_mnesia_tab(TabName, Records) ->
    emqx_mgmt_data_backup:do_restore_mnesia_tab(TabName, Records).
