%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_schema).

-behaviour(hocon_schema).

-export([
    roots/0,
    fields/1,
    namespace/0
]).

-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_plugins.hrl").

namespace() -> "plugin".

roots() -> [{?CONF_ROOT, ?HOCON(?R_REF(?CONF_ROOT), #{importance => ?IMPORTANCE_LOW})}].

fields(?CONF_ROOT) ->
    #{
        fields => root_fields(),
        desc => ?DESC(?CONF_ROOT)
    };
fields(api_endpoint) ->
    #{
        fields => api_endpoint_fields(),
        desc => ?DESC(api_endpoint)
    };
fields(state) ->
    #{
        fields => state_fields(),
        desc => ?DESC(state)
    }.

state_fields() ->
    [
        {name_vsn,
            ?HOCON(
                string(),
                #{
                    desc => ?DESC(name_vsn),
                    required => true
                }
            )},
        {enable,
            ?HOCON(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(enable),
                    required => true
                }
            )}
    ].

root_fields() ->
    [
        {states, fun states/1},
        {install_dir, fun install_dir/1},
        {api_endpoint, ?HOCON(?R_REF(api_endpoint), #{desc => ?DESC(api_endpoint)})},
        {check_interval, fun check_interval/1}
    ].

api_endpoint_fields() ->
    [
        {timeout, fun api_endpoint_timeout/1}
    ].

states(type) -> ?ARRAY(?R_REF(state));
states(required) -> false;
states(default) -> [];
states(desc) -> ?DESC(states);
states(importance) -> ?IMPORTANCE_HIGH;
states(_) -> undefined.

install_dir(type) -> string();
install_dir(required) -> false;
%% runner's root dir todo move to data dir in 5.1
install_dir(default) -> <<"plugins">>;
install_dir(desc) -> ?DESC(install_dir);
install_dir(importance) -> ?IMPORTANCE_LOW;
install_dir(_) -> undefined.

check_interval(type) -> emqx_schema:duration();
check_interval(default) -> <<"5s">>;
check_interval(desc) -> ?DESC(check_interval);
check_interval(deprecated) -> {since, "5.0.24"};
check_interval(_) -> undefined.

api_endpoint_timeout(type) -> emqx_schema:timeout_duration_ms();
api_endpoint_timeout(default) -> <<"5s">>;
api_endpoint_timeout(desc) -> ?DESC(api_endpoint_timeout);
api_endpoint_timeout(_) -> undefined.
