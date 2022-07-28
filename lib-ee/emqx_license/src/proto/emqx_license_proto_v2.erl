%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_proto_v2).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    remote_connection_counts/1,
    save_and_backup_license/2
]).

-define(TIMEOUT, 500).
-define(BACKUP_TIMEOUT, 15_000).

introduced_in() ->
    "5.0.5".

-spec remote_connection_counts(list(node())) -> list({atom(), term()}).
remote_connection_counts(Nodes) ->
    erpc:multicall(Nodes, emqx_license_resources, local_connection_count, [], ?TIMEOUT).

-spec save_and_backup_license(list(node()), binary()) -> list({atom(), term()}).
save_and_backup_license(Nodes, NewLicenseKey) ->
    erpc:multicall(Nodes, emqx_license, save_and_backup_license, [NewLicenseKey], ?BACKUP_TIMEOUT).
