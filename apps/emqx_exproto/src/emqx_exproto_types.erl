%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_types).

-include_lib("emqx_libs/include/emqx.hrl").
-include_lib("emqx_libs/include/types.hrl").

-export([serialize/2]).

-type(clientinfo() :: #{ proto_name := maybe(binary())
                       , proto_ver  := maybe(non_neg_integer())
                       , clientid   := maybe(binary())
                       , username   := maybe(binary())
                       , mountpoint := maybe(binary())
                       , keepalive  := maybe(non_neg_integer())
                       }).

-type(conninfo() :: #{ socktype := tcp | tls | udp | dtls
                     , peername := emqx_types:peername()
                     , sockname := emqx_types:sockname()
                     , peercert := nossl | binary() | list()
                     , conn_mod := atom()
                     }).

-export_type([conninfo/0, clientinfo/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(serialize(Type, Struct)
  -> term()
  when Type :: conninfo | message,
       Struct :: conninfo() | emqx_types:message()).
serialize(conninfo, #{socktype := A1,
                      peername := A2,
                      sockname := A3,
                      peercert := A4
                     }) ->
    ConnInfo = #{socktype => socktype(A1),
                 peername => address(A2),
                 sockname => address(A3)
                },
    case A4 of
        nossl ->
            ConnInfo;
        _ ->
            ConnInfo#{peercert =>
                      #{cn => esockd_peercert:common_name(A4),
                        dn => esockd_peercert:subject(A4)}
                     }
    end;

serialize(message, Msg) ->
    #{node => atom_to_binary(node(), utf8),
      id => emqx_message:id(Msg),
      qos => emqx_message:qos(Msg),
      from => emqx_message:from(Msg),
      topic => emqx_message:topic(Msg),
      payload => emqx_message:payload(Msg),
      timestamp => emqx_message:timestamp(Msg)}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

socktype(tcp) -> 'TCP';
socktype(ssl) -> 'SSL';
socktype(udp) -> 'UDP';
socktype(dtls) -> 'DTLS'.

address({Host, Port}) ->
    #{host => inet:ntoa(Host), port => Port}.
