%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_authn_schema).

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-include("emqx_gcp_device.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authn".

refs() -> [?R_REF(gcp_device)].

select_union_member(#{<<"mechanism">> := ?AUTHN_MECHANISM_BIN}) ->
    refs();
select_union_member(_Value) ->
    undefined.

fields(gcp_device) ->
    [
        {mechanism, emqx_authn_schema:mechanism(gcp_device)}
    ] ++ emqx_authn_schema:common_fields().

desc(gcp_device) ->
    ?DESC(emqx_gcp_device_api, gcp_device);
desc(_) ->
    undefined.
