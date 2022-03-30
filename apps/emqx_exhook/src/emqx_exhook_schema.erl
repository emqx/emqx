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

-behaviour(hocon_schema).

-type duration() :: integer().

-typerefl_from_string({duration/0, emqx_schema, to_duration}).

-reflect_type([duration/0]).

-export([namespace/0, roots/0, fields/1, desc/1, server_config/0]).

namespace() -> exhook.

roots() -> [exhook].

fields(exhook) ->
    [{servers,
      sc(hoconsc:array(ref(server)),
          #{ default => []
           , desc => "List of exhook servers."
           })}
    ];

fields(server) ->
    [ {name, sc(binary(),
                #{ desc => "Name of the exhook server."
                 })}
    , {enable, sc(boolean(),
                  #{ default => true
                   , desc => "Enable the exhook server."
                   })}
    , {url, sc(binary(),
               #{ desc => "URL of the gRPC server."
                })}
    , {request_timeout, sc(duration(),
                           #{ default => "5s"
                            , desc => "The timeout to request gRPC server."
                            })}
    , {failed_action, failed_action()}
    , {ssl,
       sc(ref(ssl_conf), #{})}
    , {auto_reconnect,
       sc(hoconsc:union([false, duration()]),
          #{ default => "60s"
           , desc => "Whether to automatically reconnect (initialize) the gRPC server.<br/>"
                     "When gRPC is not available, exhook tries to request the gRPC service at "
                     "that interval and reinitialize the list of mounted hooks."
           })}
    , {pool_size,
       sc(integer(),
          #{ default => 8
           , example => 8
           , desc => "The process pool size for gRPC client."
           })}
    ];

fields(ssl_conf) ->
    Schema = emqx_schema:client_ssl_opts_schema(#{}),
    lists:keydelete(user_lookup_fun, 1, Schema).

desc(exhook) ->
    "External hook (exhook) configuration.";
desc(server) ->
    "gRPC server configuration.";
desc(ssl_conf) ->
    "SSL client configuration.";
desc(_) ->
    undefined.

%% types
sc(Type, Meta) -> Meta#{type => Type}.

ref(Field) ->
    hoconsc:ref(?MODULE, Field).

failed_action() ->
    sc(hoconsc:enum([deny, ignore]),
       #{ default => deny
        , desc => "The value that is returned when the request "
                  "to the gRPC server fails for any reason."
        }).

server_config() ->
    fields(server).
