%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements the external APIs for multi-tenancy.
-module(emqx_mt).

-export([
    list_ns/0,
    list_clients/1,
    list_clients/2,
    list_clients/3,
    count_clients/1
]).

-export_type([tns/0]).

-type tns() :: binary().
-type clientid() :: emqx_types:clientid().

-define(DEFAULT_PAGE_SIZE, 100).
-define(MAX_PAGE_SIZE, 1000).

%% @doc List clients of the given tenant.
%% Starts from the beginning, with default page size 100.
-spec list_clients(tns()) -> [clientid()].
list_clients(Tns) ->
    list_clients(Tns, <<>>).

%% @doc List clients of the given tenant.
%% Starts after the given client id, with default page size 100.
-spec list_clients(tns(), clientid()) -> [clientid()].
list_clients(Tns, LastClientId) ->
    list_clients(Tns, LastClientId, ?DEFAULT_PAGE_SIZE).

%% @doc List clients of the given tenant.
%% Starts after the given client id, with the given page size.
-spec list_clients(tns(), clientid(), non_neg_integer()) -> [clientid()].
list_clients(Tns, LastClientId, PageSize) ->
    (PageSize < 1 orelse PageSize > ?MAX_PAGE_SIZE) andalso error(bad_page_size),
    emqx_mt_state:list_clients(Tns, LastClientId, PageSize).

%% @doc Count clients of the given tenant.
-spec count_clients(tns()) -> non_neg_integer().
count_clients(Tns) ->
    emqx_mt_state:count_clients(Tns).

%% @doc List all tenants.
-spec list_ns() -> [tns()].
list_ns() ->
    emqx_mt_state:list_ns().
