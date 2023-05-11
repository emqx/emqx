%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_timescale).

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"timescale">> => #{
                summary => <<"Timescale Bridge">>,
                value => emqx_bridge_pgsql:values(Method, timescale)
            }
        }
    ].

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_timescale".

roots() -> [].

fields("post") ->
    emqx_bridge_pgsql:fields("post", timescale);
fields(Method) ->
    emqx_bridge_pgsql:fields(Method).

desc(_) ->
    undefined.
