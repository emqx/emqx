%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_context).

-export([make/1, make_persist/1]).

-export_type([
    t/0,
    legacy/0,
    restricted/0
]).

-type legacy() :: emqx_types:clientinfo().
%% When trusted attributes are implemented,
%% this will be the base context for restriction
-type restricted() :: #{
    zone := emqx_types:zone() | undefined,
    protocol := emqx_types:protocol(),
    peerhost := emqx_types:peerhost(),
    sockport := non_neg_integer(),
    clientid => emqx_types:clientid(),
    username => emqx_types:username(),
    is_bridge := boolean(),
    is_superuser := boolean(),
    mountpoint := binary() | undefined,
    acl => term(),
    anonymous => boolean(),
    cert_pem => binary(),
    client_attrs => emqx_types:client_attrs(),
    cn => binary(),
    dn => binary(),
    listener => atom(),
    now_time => non_neg_integer(),
    peername => emqx_types:peername(),
    peerport => inet:port_number()
}.
-type t() :: legacy() | restricted().

-define(PERSIST_KEYS, [
    acl,
    anonymous,
    cert_pem,
    client_attrs,
    clientid,
    cn,
    dn,
    is_bridge,
    is_superuser,
    listener,
    mountpoint,
    peerhost,
    peername,
    peerport,
    protocol,
    sockport,
    username,
    zone
]).

-define(RESTRICTED_KEYS, [now_time | ?PERSIST_KEYS]).

-doc """
Create an authz context map from the client info.
""".
-spec make(emqx_types:clientinfo()) -> t().
-ifdef(TEST).
make(ClientInfo) ->
    case emqx_security_profile:policy(authz_context) of
        legacy -> ClientInfo;
        restricted -> make_restricted(ClientInfo)
    end.
-else.
%% In prod, we do not want to create a performance impact by creating authz context map.
%% But if an authz backend relies on a restricted field any test trying to use it will fail.
make(ClientInfo) ->
    ClientInfo.
-endif.

-doc """
Limit authz context to the fields relevant for persistence
""".
-spec make_persist(emqx_types:clientinfo() | t()) -> t().
make_persist(ClientInfo) ->
    maps:with(?PERSIST_KEYS, ClientInfo).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-ifdef(TEST).
make_restricted(ClientInfo) ->
    maps:with(?RESTRICTED_KEYS, ClientInfo).
-endif.
