%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_dashboard_schema).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

namespace() -> dashboard.
roots() -> ["dashboard"].

fields("dashboard") ->
    [
        {listeners,
            ?HOCON(
                ?R_REF("listeners"),
                #{desc => ?DESC(listeners)}
            )},
        {default_username, fun default_username/1},
        {default_password, fun default_password/1},
        {sample_interval,
            ?HOCON(
                emqx_schema:duration_s(),
                #{
                    default => "10s",
                    desc => ?DESC(sample_interval),
                    validator => fun validate_sample_interval/1
                }
            )},
        {token_expired_time,
            ?HOCON(
                emqx_schema:duration(),
                #{
                    default => "60m",
                    desc => ?DESC(token_expired_time)
                }
            )},
        {cors, fun cors/1},
        {i18n_lang, fun i18n_lang/1}
    ];
fields("listeners") ->
    [
        {"http",
            ?HOCON(
                ?R_REF("http"),
                #{
                    desc => "TCP listeners",
                    required => {false, recursively}
                }
            )},
        {"https",
            ?HOCON(
                ?R_REF("https"),
                #{
                    desc => "SSL listeners",
                    required => {false, recursively}
                }
            )}
    ];
fields("http") ->
    [
        enable(true),
        bind(18083)
        | common_listener_fields()
    ];
fields("https") ->
    [
        enable(false),
        bind(18084)
        | common_listener_fields() ++
            exclude_fields(
                ["fail_if_no_peer_cert"],
                emqx_schema:server_ssl_opts_schema(#{}, true)
            )
    ].

exclude_fields([], Fields) ->
    Fields;
exclude_fields([FieldName | Rest], Fields) ->
    %% assert field exists
    case lists:keytake(FieldName, 1, Fields) of
        {value, _, New} -> exclude_fields(Rest, New);
        false -> error({FieldName, Fields})
    end.

common_listener_fields() ->
    [
        {"num_acceptors",
            ?HOCON(
                integer(),
                #{
                    default => 4,
                    desc => ?DESC(num_acceptors)
                }
            )},
        {"max_connections",
            ?HOCON(
                integer(),
                #{
                    default => 512,
                    desc => ?DESC(max_connections)
                }
            )},
        {"backlog",
            ?HOCON(
                integer(),
                #{
                    default => 1024,
                    desc => ?DESC(backlog)
                }
            )},
        {"send_timeout",
            ?HOCON(
                emqx_schema:duration(),
                #{
                    default => "5s",
                    desc => ?DESC(send_timeout)
                }
            )},
        {"inet6",
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(inet6)
                }
            )},
        {"ipv6_v6only",
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(ipv6_v6only)
                }
            )}
    ].

enable(Bool) ->
    {"enable",
        ?HOCON(
            boolean(),
            #{
                default => Bool,
                required => true,
                desc => ?DESC(listener_enable)
            }
        )}.

bind(Port) ->
    {"bind",
        ?HOCON(
            ?UNION([non_neg_integer(), emqx_schema:ip_port()]),
            #{
                default => Port,
                required => true,
                example => "0.0.0.0:" ++ integer_to_list(Port),
                desc => ?DESC(bind)
            }
        )}.

desc("dashboard") ->
    ?DESC(desc_dashboard);
desc("listeners") ->
    ?DESC(desc_listeners);
desc("http") ->
    ?DESC(desc_http);
desc("https") ->
    ?DESC(desc_https);
desc(_) ->
    undefined.

default_username(type) -> binary();
default_username(default) -> "admin";
default_username(required) -> true;
default_username(desc) -> ?DESC(default_username);
default_username('readOnly') -> true;
default_username(_) -> undefined.

default_password(type) -> binary();
default_password(default) -> "public";
default_password(required) -> true;
default_password('readOnly') -> true;
default_password(sensitive) -> true;
default_password(desc) -> ?DESC(default_password);
default_password(_) -> undefined.

cors(type) -> boolean();
cors(default) -> false;
cors(required) -> false;
cors(desc) -> ?DESC(cors);
cors(_) -> undefined.

i18n_lang(type) -> ?ENUM([en, zh]);
i18n_lang(default) -> en;
i18n_lang('readOnly') -> true;
i18n_lang(desc) -> ?DESC(i18n_lang);
i18n_lang(_) -> undefined.

validate_sample_interval(Second) ->
    case Second >= 1 andalso Second =< 60 andalso (60 rem Second =:= 0) of
        true ->
            ok;
        false ->
            Msg = "must be between 1 and 60 and be a divisor of 60.",
            {error, Msg}
    end.
