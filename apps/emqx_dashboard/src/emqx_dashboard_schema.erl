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

namespace() -> <<"dashboard">>.
roots() -> ["dashboard"].

fields("dashboard") ->
    [
        {listeners,
            sc(
                ref("listeners"),
                #{
                    desc =>
                        "HTTP(s) listeners are identified by their protocol type and are\n"
                        "used to serve dashboard UI and restful HTTP API.<br>\n"
                        "Listeners must have a unique combination of port number and IP address.<br>\n"
                        "For example, an HTTP listener can listen on all configured IP addresses\n"
                        "on a given port for a machine by specifying the IP address 0.0.0.0.<br>\n"
                        "Alternatively, the HTTP listener can specify a unique IP address for each listener,\n"
                        "but use the same port."
                }
            )},
        {default_username, fun default_username/1},
        {default_password, fun default_password/1},
        {sample_interval,
            sc(
                emqx_schema:duration_s(),
                #{
                    default => "10s",
                    desc =>
                        "How often to update metrics displayed in the dashboard.<br/>"
                        "Note: `sample_interval` should be a divisor of 60."
                }
            )},
        {token_expired_time,
            sc(
                emqx_schema:duration(),
                #{
                    default => "30m",
                    desc => "JWT token expiration time."
                }
            )},
        {cors, fun cors/1},
        {i18n_lang, fun i18n_lang/1}
    ];
fields("listeners") ->
    [
        {"http",
            sc(
                ref("http"),
                #{
                    desc => "TCP listeners",
                    required => {false, recursively}
                }
            )},
        {"https",
            sc(
                ref("https"),
                #{
                    desc => "SSL listeners",
                    required => {false, recursively}
                }
            )}
    ];
fields("http") ->
    [
        {"bind", fun bind/1},
        {"num_acceptors",
            sc(
                integer(),
                #{
                    default => 4,
                    desc => "Socket acceptor pool size for TCP protocols."
                }
            )},
        {"max_connections",
            sc(
                integer(),
                #{
                    default => 512,
                    desc => "Maximum number of simultaneous connections."
                }
            )},
        {"backlog",
            sc(
                integer(),
                #{
                    default => 1024,
                    desc =>
                        "Defines the maximum length that the queue of pending connections can grow to."
                }
            )},
        {"send_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    default => "5s",
                    desc => "Send timeout for the socket."
                }
            )},
        {"inet6",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Sets up the listener for IPv6."
                }
            )},
        {"ipv6_v6only",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Disable IPv4-to-IPv6 mapping for the listener."
                }
            )}
    ];
fields("https") ->
    fields("http") ++
        proplists:delete(
            "fail_if_no_peer_cert",
            emqx_schema:server_ssl_opts_schema(#{}, true)
        ).

desc("dashboard") ->
    "Configuration for EMQX dashboard.";
desc("http") ->
    "Configuration for the dashboard listener (plaintext).";
desc("https") ->
    "Configuration for the dashboard listener (TLS).";
desc(_) ->
    undefined.

bind(type) -> hoconsc:union([non_neg_integer(), emqx_schema:ip_port()]);
bind(default) -> 18083;
bind(required) -> true;
bind(desc) -> "Port without IP(18083) or port with specified IP(127.0.0.1:18083).";
bind(_) -> undefined.

default_username(type) -> string();
default_username(default) -> "admin";
default_username(required) -> true;
default_username(desc) -> "The default username of the automatically created dashboard user.";
default_username('readOnly') -> true;
default_username(_) -> undefined.

default_password(type) ->
    string();
default_password(default) ->
    "public";
default_password(required) ->
    true;
default_password('readOnly') ->
    true;
default_password(sensitive) ->
    true;
default_password(desc) ->
    ""
    "\n"
    "The initial default password for dashboard 'admin' user.\n"
    "For safety, it should be changed as soon as possible."
    "";
default_password(_) ->
    undefined.

cors(type) ->
    boolean();
cors(default) ->
    false;
cors(required) ->
    false;
cors(desc) ->
    "Support Cross-Origin Resource Sharing (CORS).\n"
    "Allows a server to indicate any origins (domain, scheme, or port) other than\n"
    "its own from which a browser should permit loading resources.";
cors(_) ->
    undefined.

i18n_lang(type) -> ?ENUM([en, zh]);
i18n_lang(default) -> zh;
i18n_lang('readOnly') -> true;
i18n_lang(desc) -> "Internationalization language support.";
i18n_lang(_) -> undefined.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(Field) -> hoconsc:ref(?MODULE, Field).
