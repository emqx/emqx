%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ocpp_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-define(DEFAULT_MOUNTPOINT, <<"ocpp/">>).

%% config schema provides
-export([namespace/0, roots/0, fields/1, desc/1]).

namespace() -> "gateway_ocpp".

roots() -> [].

fields(ocpp) ->
    [
        {mountpoint, emqx_gateway_schema:mountpoint(?DEFAULT_MOUNTPOINT)},
        {default_heartbeat_interval,
            sc(
                emqx_schema:duration_s(),
                #{
                    default => <<"60s">>,
                    required => true,
                    desc => ?DESC(default_heartbeat_interval)
                }
            )},
        {heartbeat_checking_times_backoff,
            sc(
                integer(),
                #{
                    default => 1,
                    required => false,
                    desc => ?DESC(heartbeat_checking_times_backoff)
                }
            )},
        {upstream, sc(ref(upstream), #{})},
        {dnstream, sc(ref(dnstream), #{})},
        {message_format_checking,
            sc(
                hoconsc:union([all, upstream_only, dnstream_only, disable]),
                #{
                    default => disable,
                    desc => ?DESC(message_format_checking)
                }
            )},
        {json_schema_dir,
            sc(
                string(),
                #{
                    default => <<"${application_priv}/schemas">>,
                    desc => ?DESC(json_schema_dir)
                }
            )},
        {json_schema_id_prefix,
            sc(
                string(),
                #{
                    default => <<"urn:OCPP:1.6:2019:12:">>,
                    desc => ?DESC(json_schema_id_prefix)
                }
            )},
        {listeners, sc(ref(ws_listeners), #{})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(ws_listeners) ->
    [
        {ws, sc(map(name, ref(ws_listener)), #{desc => ?DESC(ws)})},
        {wss, sc(map(name, ref(wss_listener)), #{desc => ?DESC(wss)})}
    ];
fields(ws_listener) ->
    emqx_gateway_schema:ws_listener() ++
        [{websocket, sc(ref(websocket), #{})}];
fields(wss_listener) ->
    emqx_gateway_schema:wss_listener() ++
        [{websocket, sc(ref(websocket), #{})}];
fields(websocket) ->
    DefaultPath = <<"/ocpp">>,
    SubProtocols = <<"ocpp1.6, ocpp2.0">>,
    emqx_gateway_schema:ws_opts(DefaultPath, SubProtocols);
fields(upstream) ->
    [
        {topic,
            sc(
                string(),
                #{
                    required => true,
                    default => <<"cp/${cid}">>,
                    desc => ?DESC(upstream_topic)
                }
            )},
        {topic_override_mapping,
            sc(
                %% XXX: more clearly type defination
                hoconsc:map(name, string()),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(upstream_topic_override_mapping)
                }
            )},
        {reply_topic,
            sc(
                string(),
                #{
                    required => true,
                    default => <<"cp/${cid}/Reply">>,
                    desc => ?DESC(upstream_reply_topic)
                }
            )},
        {error_topic,
            sc(
                string(),
                #{
                    required => true,
                    default => <<"cp/${cid}/Reply">>,
                    desc => ?DESC(upstream_error_topic)
                }
            )}
        %{awaiting_timeout,
        %    sc(
        %        emqx_schema:duration(),
        %        #{
        %            required => false,
        %            default => <<"30s">>,
        %            desc => ?DESC(upstream_awaiting_timeout)
        %         }
        %     )}
    ];
fields(dnstream) ->
    [
        %%{strit_mode,
        %%    sc(
        %%        boolean(),
        %%        #{
        %%            required => false,
        %%            default => false,
        %%            desc => ?DESC(dnstream_strit_mode)
        %%        }
        %%    )},
        {topic,
            sc(
                string(),
                #{
                    required => true,
                    default => <<"cs/${cid}">>,
                    desc => ?DESC(dnstream_topic)
                }
            )},
        %{retry_interval,
        %    sc(
        %        emqx_schema:duration(),
        %        #{
        %            required => false,
        %            default => <<"30s">>,
        %            desc => ?DESC(dnstream_retry_interval)
        %         }
        %     )},
        {max_mqueue_len,
            sc(
                integer(),
                #{
                    required => false,
                    default => 100,
                    desc => ?DESC(dnstream_max_mqueue_len)
                }
            )}
    ].

desc(ocpp) ->
    "The OCPP gateway";
desc(upstream) ->
    "Upload stream topic to notify third-party system what's messages/events reported by "
    "Charge Point. Available placeholders:\n"
    "- <code>cid</code>: Charge Point ID\n"
    "- <code>clientid</code>: Equal to Charge Point ID\n"
    "- <code>action</code>: Message Name in OCPP";
desc(dnstream) ->
    "Download stream topic to forward the system message to device. Available placeholders:\n"
    "- <code>cid</code>: Charge Point ID\n"
    "- <code>clientid</code>: Equal to Charge Point ID\n"
    "- <code>action</code>: Message Name in OCPP";
desc(ws_listeners) ->
    "Websocket listeners";
desc(ws_listener) ->
    "Websocket listener";
desc(wss_listener) ->
    "Websocket over TLS listener";
desc(websocket) ->
    "Websocket options";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

map(Name, Type) ->
    hoconsc:map(Name, Type).

ref(Field) ->
    hoconsc:ref(?MODULE, Field).
