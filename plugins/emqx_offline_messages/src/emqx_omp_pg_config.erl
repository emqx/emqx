%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_config).
-moduledoc "Validate the PostgreSQL plugin configuration.".
-behaviour(hocon_schema).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-export([namespace/0, roots/0, fields/1, desc/1, parse/1]).

namespace() -> omp_pg.
roots() -> [config].
desc(_) -> undefined.

fields(config) ->
    [
        {enable, mk(boolean(), false)},
        {host, mk(binary(), <<"127.0.0.1">>)},
        {port, mk(range(1, 65535), 5432)},
        {database, mk(binary(), <<"mqtt">>)},
        {username, mk(binary(), <<"root">>)},
        {password, hoconsc:mk(binary(), #{default => <<"public">>, sensitive => true})},
        {ssl, hoconsc:mk(hoconsc:ref(emqx_schema, "ssl_client_opts"), #{})},
        {namespace, mk(binary(), <<>>)},
        {topics, mk(hoconsc:array(binary()), [])},
        {init_default_schema, mk(boolean(), false)},
        {max_topic_length, mk(range(1, 2048), 1024)},
        {new_subscription_start, mk(hoconsc:enum([earliest, latest]), earliest)},
        {query_timeout, mk(pos_integer(), 5000)},
        {set_statement_timeout, mk(boolean(), true)},
        {write_pool_size, mk(range(1, 128), 8)},
        {read_pool_size, mk(range(1, 128), 8)},
        {writer, mk(hoconsc:ref(?MODULE, writer), #{})},
        {read, mk(hoconsc:ref(?MODULE, read), #{})},
        {checkpoint, mk(hoconsc:ref(?MODULE, checkpoint), #{})},
        {retention, mk(hoconsc:ref(?MODULE, retention), #{})},
        {subscription_expiry, mk(pos_integer(), 2592000000)}
    ];
fields(writer) ->
    [
        {pool_size, mk(range(1, 128), 4)},
        {min_flush_timeout, mk(pos_integer(), 5)},
        {max_flush_timeout, mk(pos_integer(), 50)},
        {flush_max_messages, mk(pos_integer(), 1000)},
        {flush_max_bytes, mk(pos_integer(), 1048576)},
        {call_timeout, mk(pos_integer(), 5000)}
    ];
fields(read) ->
    [
        {page_max_messages, mk(pos_integer(), 100)},
        {window, mk(pos_integer(), 1000)},
        {reorder_max_distance, mk(pos_integer(), 32)},
        {reorder_timeout, mk(pos_integer(), 50)},
        {idle_poll_interval, mk(pos_integer(), 30000)}
    ];
fields(checkpoint) ->
    [{interval, mk(pos_integer(), 1000)}, {max_pending, mk(pos_integer(), 100)}];
fields(retention) ->
    [
        {max_age, mk(pos_integer(), 604800000)},
        {max_messages, mk(pos_integer(), 1000000)},
        {interval, mk(pos_integer(), 60000)}
    ].

mk(Type, Default) -> hoconsc:mk(Type, #{default => Default}).

parse(Raw) ->
    #{<<"ssl">> := SSL0} = Raw0 = emqx_offline_messages_utils:fix_ssl_config(Raw),
    SSL1 = maps:merge(
        #{<<"verify">> => <<"verify_peer">>, <<"server_name_indication">> => <<"auto">>}, SSL0
    ),
    Config0 = emqx_offline_messages_utils:check_config(
        ?MODULE,
        Raw0#{<<"ssl">> := SSL1}
    ),
    #{
        host := Host,
        port := Port,
        username := Username,
        password := Password,
        database := Database,
        ssl := SSL,
        query_timeout := Timeout,
        namespace := Namespace,
        topics := Topics,
        writer := #{min_flush_timeout := Min, max_flush_timeout := Max}
    } = Config0,
    true = byte_size(Namespace) =< 256,
    true = Min =< Max,
    lists:foreach(fun(Topic) -> true = emqx_topic:validate(filter, Topic) end, Topics),
    SSLMode =
        case SSL of
            #{enable := true} -> required;
            _ -> false
        end,
    ClientSSL =
        case SSL of
            #{server_name_indication := "auto"} -> SSL#{server_name_indication := Host};
            _ -> SSL
        end,
    Connection = #{
        host => binary_to_list(Host),
        port => Port,
        username => binary_to_list(Username),
        password => binary_to_list(Password),
        database => binary_to_list(Database),
        ssl => SSLMode,
        ssl_opts => emqx_tls_lib:to_client_opts(ClientSSL),
        timeout => Timeout
    },
    Config0#{connection => Connection, instance => make_ref()}.
