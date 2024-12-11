%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements the external APIs for multi-tenancy.
-module(emqx_mt).

-export([
    list_ns/0,
    list_ns/2,
    list_clients/1,
    list_clients/2,
    list_clients/3,
    count_clients/1
]).

-export_type([tns/0]).

-include("emqx_mt.hrl").

-type tns() :: binary().
-type clientid() :: emqx_types:clientid().

%% @doc List clients of the given namespace.
%% Starts from the beginning, with default page size 100.
-spec list_clients(tns()) -> {ok, [clientid()]} | {error, not_found}.
list_clients(Tns) ->
    list_clients(Tns, ?MIN_CLIENTID).

%% @doc List clients of the given tenant.
%% Starts after the given client id, with default page size 100.
-spec list_clients(tns(), clientid()) -> {ok, [clientid()]} | {error, not_found}.
list_clients(Tns, LastClientId) ->
    list_clients(Tns, LastClientId, ?DEFAULT_PAGE_SIZE).

%% @doc List clients of the given tenant.
%% Starts after the given client id, with the given page size.
-spec list_clients(tns(), clientid(), non_neg_integer()) -> {ok, [clientid()]} | {error, not_found}.
list_clients(Tns, LastClientId, PageSize) ->
    (PageSize < 1 orelse PageSize > ?MAX_PAGE_SIZE) andalso error(bad_page_size),
    emqx_mt_state:list_clients(Tns, LastClientId, PageSize).

%% @doc Count clients of the given tenant.
%% `{error, not_found}' is returned if there is not any client found.
-spec count_clients(tns()) -> {ok, non_neg_integer()} | {error, not_found}.
count_clients(Tns) ->
    emqx_mt_state:count_clients(Tns).

%% @doc List first page of namespaces.
%% Default page size is 100.
-spec list_ns() -> [tns()].
list_ns() ->
    list_ns(?MIN_NS, ?DEFAULT_PAGE_SIZE).

%% @doc List namespaces skipping the last namespace from the previous page.
%% The second argument is the number of namespaces to return.
list_ns(LastNs, Limit) ->
    emqx_mt_state:list_ns(LastNs, Limit).
