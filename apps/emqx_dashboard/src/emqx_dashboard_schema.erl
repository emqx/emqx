%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    desc/1,
    https_converter/2
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
                emqx_schema:timeout_duration_s(),
                #{
                    default => <<"10s">>,
                    desc => ?DESC(sample_interval),
                    importance => ?IMPORTANCE_HIDDEN,
                    validator => fun validate_sample_interval/1
                }
            )},
        {token_expired_time,
            ?HOCON(
                emqx_schema:duration(),
                #{
                    default => <<"60m">>,
                    desc => ?DESC(token_expired_time)
                }
            )},
        {password_expired_time,
            ?HOCON(
                emqx_schema:duration_s(),
                #{
                    default => 0,
                    desc => ?DESC(password_expired_time)
                }
            )},
        {cors, fun cors/1},
        {swagger_support, fun swagger_support/1},
        {i18n_lang, fun i18n_lang/1},
        {bootstrap_users_file,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(bootstrap_users_file),
                    required => false,
                    default => <<>>,
                    deprecated => {since, "5.1.0"},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ] ++ sso_fields();
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
                    required => {false, recursively},
                    converter => fun ?MODULE:https_converter/2
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
        bind(18084),
        ssl_options()
        | common_listener_fields()
    ];
fields("ssl_options") ->
    server_ssl_options().

ssl_options() ->
    {"ssl_options",
        ?HOCON(
            ?R_REF("ssl_options"),
            #{
                required => true,
                desc => ?DESC(ssl_options),
                importance => ?IMPORTANCE_HIGH
            }
        )}.

server_ssl_options() ->
    emqx_schema:server_ssl_opts_schema(#{}, true).

common_listener_fields() ->
    [
        {"num_acceptors",
            ?HOCON(
                integer(),
                #{
                    default => erlang:system_info(schedulers_online),
                    desc => ?DESC(num_acceptors),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"max_connections",
            ?HOCON(
                integer(),
                #{
                    default => 512,
                    desc => ?DESC(max_connections),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {"backlog",
            ?HOCON(
                integer(),
                #{
                    default => 1024,
                    desc => ?DESC(backlog),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"send_timeout",
            ?HOCON(
                emqx_schema:duration(),
                #{
                    default => <<"10s">>,
                    desc => ?DESC(send_timeout),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"inet6",
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(inet6),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"ipv6_v6only",
            ?HOCON(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(ipv6_v6only),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"proxy_header",
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(proxy_header),
                    default => false,
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ].

enable(Bool) ->
    {"enable",
        ?HOCON(
            boolean(),
            #{
                default => Bool,
                required => false,
                %% deprecated because we use port number =:= 0 to disable
                deprecated => {since, "5.1.0"},
                importance => ?IMPORTANCE_HIDDEN,
                desc => ?DESC(listener_enable)
            }
        )}.

bind(Port) ->
    {"bind",
        ?HOCON(
            emqx_schema:ip_port(),
            #{
                default => 0,
                required => false,
                example => "0.0.0.0:" ++ integer_to_list(Port),
                importance => ?IMPORTANCE_HIGH,
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
desc("ssl_options") ->
    ?DESC(ssl_options);
desc(_) ->
    undefined.

default_username(type) -> binary();
default_username(default) -> <<"admin">>;
default_username(required) -> true;
default_username(desc) -> ?DESC(default_username);
default_username('readOnly') -> true;
%% username is hidden but password is not,
%% this is because we want to force changing 'admin' user's password.
%% instead of suggesting to create a new user --- which could be
%% more prone to leaving behind 'admin' user's password unchanged without detection.
default_username(importance) -> ?IMPORTANCE_HIDDEN;
default_username(_) -> undefined.

default_password(type) -> binary();
default_password(default) -> <<"public">>;
default_password(required) -> true;
default_password('readOnly') -> true;
default_password(sensitive) -> true;
default_password(converter) -> fun emqx_schema:password_converter/2;
default_password(desc) -> ?DESC(default_password);
default_password(importance) -> ?IMPORTANCE_LOW;
default_password(_) -> undefined.

cors(type) -> boolean();
cors(default) -> false;
cors(required) -> false;
cors(desc) -> ?DESC(cors);
cors(_) -> undefined.

swagger_support(type) -> boolean();
swagger_support(default) -> true;
swagger_support(desc) -> ?DESC(swagger_support);
swagger_support(_) -> undefined.

%% TODO: change it to string type
%% It will be up to the dashboard package which languages to support
i18n_lang(type) -> ?ENUM([en, zh]);
i18n_lang(default) -> en;
i18n_lang('readOnly') -> true;
i18n_lang(desc) -> ?DESC(i18n_lang);
i18n_lang(importance) -> ?IMPORTANCE_HIDDEN;
i18n_lang(_) -> undefined.

validate_sample_interval(Second) ->
    case Second >= 1 andalso Second =< 60 andalso (60 rem Second =:= 0) of
        true ->
            ok;
        false ->
            Msg = "must be between 1 and 60 and be a divisor of 60.",
            {error, Msg}
    end.

https_converter(undefined, _Opts) ->
    %% no https listener configured
    undefined;
https_converter(Conf, Opts) ->
    convert_ssl_layout(Conf, Opts).

convert_ssl_layout(Conf = #{<<"ssl_options">> := _}, _Opts) ->
    Conf;
convert_ssl_layout(Conf = #{}, _Opts) ->
    Keys = lists:map(fun({K, _}) -> list_to_binary(K) end, server_ssl_options()),
    SslOpts = maps:with(Keys, Conf),
    Conf1 = maps:without(Keys, Conf),
    Conf1#{<<"ssl_options">> => SslOpts}.

-if(?EMQX_RELEASE_EDITION == ee).
sso_fields() ->
    [
        {sso,
            ?HOCON(
                ?R_REF(emqx_dashboard_sso_schema, sso),
                #{required => {false, recursively}}
            )}
    ].

-else.
sso_fields() ->
    [].
-endif.
