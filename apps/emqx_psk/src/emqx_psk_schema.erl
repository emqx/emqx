%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_psk_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").

-export([ roots/0
        , fields/1
        ]).

-import(emqx_schema, [sc/2]).

roots() -> ["psk_authentication"].

fields("psk_authentication") ->
    #{fields => fields(),
      desc => """PSK stands for 'Pre-Shared Keys'.
This config to enable TLS-PSK authentication.

<strong>Important!</strong> Make sure the SSL listener with
only <code>tlsv1.2</code> enabled, and also PSK cipher suites
configured, such as <code>RSA-PSK-AES256-GCM-SHA384</code>.
See listener SSL options config for more details.

The IDs and secrets can be provided from a file the path
to which is configurable by the <code>init_file</code> field.
"""
     }.

fields() ->
    [ {enable, sc(boolean(), #{default => false,
                               desc => <<"Whether to enable TLS PSK support">>
                              })}
    , {init_file, sc(binary(),
                     #{required => false,
                       desc =>
                           <<"If init_file is specified, emqx will import PSKs from the file ",
                             "into the built-in database at startup for use by the runtime. ",
                             "The file has to be structured line-by-line, each line must be in ",
                             "the format of <code>PSKIdentity:SharedSecret</code>. For example: ",
                             "<code>mydevice1:c2VjcmV0</code>">>
                      })}
    , {separator, sc(binary(),
                     #{default => <<":">>,
                       desc =>
                           <<"The separator between <code>PSKIdentity</code>"
                             " and <code>SharedSecret</code> in the psk file">>
                      })}
    , {chunk_size, sc(integer(),
                      #{default => 50,
                        desc => <<"The size of each chunk used to import to"
                                  " the built-in database from psk file">>
                       })}
    ].
