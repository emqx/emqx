%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).

-export([namespace/0, roots/0, fields/1, desc/1, server_config/0]).

namespace() -> exhook.

roots() -> [exhook].

fields(exhook) ->
    [
        {servers,
            ?HOCON(?ARRAY(?R_REF(server)), #{
                default => [],
                desc => ?DESC(servers)
            })}
    ];
fields(server) ->
    [
        {name,
            ?HOCON(binary(), #{
                example => <<"default">>,
                required => true,
                validator => fun validate_name/1,
                desc => ?DESC(name)
            })},
        {enable,
            ?HOCON(boolean(), #{
                default => true,
                desc => ?DESC(enable)
            })},
        {url,
            ?HOCON(binary(), #{
                required => true,
                desc => ?DESC(url),
                example => <<"http://127.0.0.1:9000">>
            })},
        {request_timeout,
            ?HOCON(emqx_schema:duration(), #{
                default => "5s",
                desc => ?DESC(request_timeout)
            })},
        {failed_action, failed_action()},
        {ssl, ?HOCON(?R_REF(ssl_conf), #{})},
        {socket_options,
            ?HOCON(?R_REF(socket_options), #{
                default => #{<<"keepalive">> => true, <<"nodelay">> => true}
            })},
        {auto_reconnect,
            ?HOCON(hoconsc:union([false, emqx_schema:duration()]), #{
                default => "60s",
                desc => ?DESC(auto_reconnect)
            })},
        {pool_size,
            ?HOCON(pos_integer(), #{
                default => 8,
                desc => ?DESC(pool_size)
            })}
    ];
fields(ssl_conf) ->
    Schema = emqx_schema:client_ssl_opts_schema(#{}),
    lists:keydelete("user_lookup_fun", 1, Schema);
fields(socket_options) ->
    [
        {keepalive, ?HOCON(boolean(), #{default => true, desc => ?DESC(keepalive)})},
        {nodelay, ?HOCON(boolean(), #{default => true, desc => ?DESC(nodelay)})},
        {recbuf,
            ?HOCON(emqx_schema:bytesize(), #{
                desc => ?DESC(recbuf), required => false, example => <<"64KB">>
            })},
        {sndbuf,
            ?HOCON(emqx_schema:bytesize(), #{
                desc => ?DESC(sndbuf), required => false, example => <<"16KB">>
            })}
    ].

desc(exhook) ->
    "External hook (exhook) configuration.";
desc(server) ->
    "gRPC server configuration.";
desc(ssl_conf) ->
    "SSL client configuration.";
desc(socket_options) ->
    ?DESC(socket_options);
desc(_) ->
    undefined.

failed_action() ->
    ?HOCON(?ENUM([deny, ignore]), #{
        default => deny,
        desc => ?DESC(failed_action)
    }).

validate_name(Name) ->
    NameRE = "^[A-Za-z0-9]+[A-Za-z0-9-_]*$",
    NameLen = byte_size(Name),
    case NameLen > 0 andalso NameLen =< 256 of
        true ->
            case re:run(Name, NameRE) of
                {match, _} ->
                    ok;
                _NoMatch ->
                    Reason = list_to_binary(
                        io_lib:format("Bad ExHook Name ~p, expect ~p", [Name, NameRE])
                    ),
                    {error, Reason}
            end;
        false ->
            {error, "Name Length must =< 256"}
    end.

server_config() ->
    fields(server).
