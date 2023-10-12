%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_authz_enterprise).

-export([
    type_names/0,
    fields/1,
    is_enterprise_module/1,
    authz_sources_types/0,
    type/1,
    desc/1
]).

-dialyzer({nowarn_function, [fields/1, type/1, desc/1]}).

-if(?EMQX_RELEASE_EDITION == ee).

%% type name set
type_names() ->
    [].

%% type -> type schema
fields(Any) ->
    error({invalid_field, Any}).

%% type -> type module
is_enterprise_module(_) ->
    false.

%% api sources set
authz_sources_types() ->
    [].

%% atom-able name -> type
type(Unknown) -> throw({unknown_authz_source_type, Unknown}).

desc(_) ->
    undefined.

-else.

type_names() ->
    [].

fields(Any) ->
    error({invalid_field, Any}).

is_enterprise_module(_) ->
    false.

authz_sources_types() ->
    [].

%% should never happen if the input is type-checked by hocon schema
type(Unknown) -> throw({unknown_authz_source_type, Unknown}).

desc(_) ->
    undefined.
-endif.
