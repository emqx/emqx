%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_nats_connector).

-behaviour(emqx_resource).
-behaviour(ecpool_worker).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_bridge_nats.hrl").

-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_query/3,
    on_batch_query/3
]).
-export([pre_config_update/4]).
-export([connect/1, publish/3, publish_batch/3]).

%%--------------------------------------------------------------------
%% Connector lifecycle
%%--------------------------------------------------------------------

resource_type() -> ?CONNECTOR_TYPE.
callback_mode() -> always_sync.

pre_config_update(Path, _Name, Config, _OldConfig) ->
    emqx_bridge_nats_credentials:materialize(Path, Config).

on_start(InstanceId, Config) ->
    case client_options(Config) of
        {error, Reason} ->
            {error, {invalid_config, Reason}};
        ClientOpts ->
            PoolOpts = [
                {pool, InstanceId},
                {pool_size, maps:get(pool_size, Config, 8)},
                {config, ClientOpts}
            ],
            case emqx_resource_pool:start(InstanceId, ?MODULE, PoolOpts) of
                ok -> {ok, #{pool => InstanceId, channels => #{}}};
                {error, Reason} -> {error, Reason}
            end
    end.

on_stop(InstanceId, _State) ->
    emqx_resource_pool:stop(InstanceId).

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_add_channel(
    _InstanceId,
    #{channels := Channels} = State,
    ChannelId,
    #{parameters := Params} = ChannelConfig
) ->
    Subject = maps:get(subject, Params),
    PayloadTemplate = emqx_template:parse(
        maps:get(payload_template, Params, <<"$", "{.payload}">>)
    ),
    HeaderTemplates = [
        {
            emqx_template:parse(maps:get(key, Header)),
            emqx_template:parse(maps:get(value, Header))
        }
     || Header <- maps:get(headers, Params, [])
    ],
    ResourceOpts = maps:get(resource_opts, ChannelConfig, #{}),
    RequestTTL = maps:get(request_ttl, ResourceOpts, 5000),
    Channel = #{
        subject => emqx_placeholder:preproc_tmpl(Subject),
        payload => PayloadTemplate,
        headers => HeaderTemplates,
        delivery_mode => maps:get(delivery_mode, Params, core),
        msg_id => emqx_placeholder:preproc_tmpl(maps:get(msg_id_template, Params, <<>>)),
        request_ttl => RequestTTL
    },
    {ok, State#{channels => maps:put(ChannelId, Channel, Channels)}}.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannelId) ->
    {ok, State#{channels => maps:remove(ChannelId, Channels)}}.

on_get_channel_status(InstanceId, _ChannelId, State) ->
    on_get_status(InstanceId, State).

on_get_status(InstanceId, _State) ->
    emqx_resource_pool:common_health_check_workers(
        InstanceId,
        #{
            check_fn => fun health_check/1,
            is_success_fn => fun
                (ok) -> false;
                (_) -> true
            end
        }
    ).

health_check(Client) ->
    enats_client:flush(Client, 1000).

%%--------------------------------------------------------------------
%% Resource callbacks
%%--------------------------------------------------------------------

on_query(InstanceId, {ChannelId, Message}, #{channels := Channels}) ->
    case maps:find(ChannelId, Channels) of
        {ok, Channel} ->
            RawResult = ecpool:pick_and_do(
                InstanceId, {?MODULE, publish, [Channel, Message]}, no_handover
            ),
            Result = classify_result(RawResult),
            ?tp(
                nats_connector_query_return,
                #{instance_id => InstanceId, result => Result}
            ),
            Result;
        error ->
            {error, {unrecoverable_error, {invalid_channel, ChannelId}}}
    end;
on_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

on_batch_query(InstanceId, [{_ChannelId, _Message} | _] = Batch, State) ->
    [{ChannelId, _} | _] = Batch,
    case maps:find(ChannelId, maps:get(channels, State)) of
        {ok, Channel} ->
            case lists:all(fun({ChannelId0, _}) -> ChannelId0 =:= ChannelId end, Batch) of
                true ->
                    RawResult = ecpool:pick_and_do(
                        InstanceId, {?MODULE, publish_batch, [Channel, Batch]}, no_handover
                    ),
                    Result = classify_result(RawResult),
                    ?tp(nats_connector_query_return, #{
                        instance_id => InstanceId,
                        batch => true,
                        batch_size => length(Batch),
                        result => Result
                    }),
                    Result;
                false ->
                    {error, {unrecoverable_error, mixed_channels_in_batch}}
            end;
        error ->
            {error, {unrecoverable_error, {invalid_channel, ChannelId}}}
    end;
on_batch_query(_InstanceId, Batch, _State) ->
    {error, {unrecoverable_error, {invalid_batch, Batch}}}.

%%--------------------------------------------------------------------
%% NATS connection and publishing
%%--------------------------------------------------------------------

connect(Options) ->
    ClientOpts = proplists:get_value(config, Options),
    case enats_client:start_link(ClientOpts#{owner => self(), reconnect => true}) of
        {ok, Client} ->
            case enats_client:connect(Client, maps:get(connect_timeout, ClientOpts, 5000)) of
                ok ->
                    {ok, Client};
                {error, Reason} ->
                    _ = enats_client:stop(Client),
                    {error, Reason}
            end;
        Error ->
            Error
    end.

publish(Client, Channel, Message) ->
    publish_message(Client, Channel, Message, true).

publish_batch(Client, Channel, Batch) ->
    case maps:get(delivery_mode, Channel, core) of
        core ->
            publish_core_batch(Client, Channel, Batch);
        jetstream ->
            publish_jetstream_batch(Client, Channel, Batch)
    end.

publish_core_batch(Client, Channel, Batch) ->
    IndexedBatch = lists:zip(lists:seq(1, length(Batch)), Batch),
    {Valid, Results0} = render_batch(IndexedBatch, Channel, [], #{}),
    publish_core_batch(Client, Channel, Valid, Results0).

publish_core_batch(_Client, _Channel, [], Results) ->
    results_in_order(Results);
publish_core_batch(Client, Channel, Valid, Results0) ->
    RequestTTL = maps:get(request_ttl, Channel, 5000),
    Deadline = request_deadline(RequestTTL),
    WireMessages = [rendered_to_batch_message(Rendered) || {_Index, Rendered} <- Valid],
    case enats_client:publish_batch(Client, WireMessages, remaining_timeout(Deadline)) of
        ok ->
            case enats_client:flush(Client, remaining_timeout(Deadline)) of
                ok -> results_in_order(Results0);
                {error, _} = Error -> Error
            end;
        {error, {invalid_batch_message, BadIndex, Reason}} ->
            {OriginalIndex, _Rendered} = lists:nth(BadIndex, Valid),
            Valid1 = remove_nth(BadIndex, Valid),
            Results1 = Results0#{OriginalIndex => {error, Reason}},
            publish_core_batch(Client, Channel, Valid1, Results1);
        {error, _} = Error ->
            Error
    end.

publish_jetstream_batch(Client, Channel, Batch) ->
    case publish_batch_messages(Client, Channel, Batch, []) of
        {recoverable, Reason} ->
            {error, Reason};
        {ok, Results} ->
            Results
    end.

%%--------------------------------------------------------------------
%% Message rendering and batch handling
%%--------------------------------------------------------------------

render_batch([], _Channel, Valid, Results) ->
    {lists:reverse(Valid), Results};
render_batch([{Index, {_ChannelId, Message}} | Rest], Channel, Valid, Results0) ->
    case render_message(Channel, Message) of
        {ok, Rendered} ->
            render_batch(Rest, Channel, [{Index, Rendered} | Valid], Results0#{Index => ok});
        {error, Reason} ->
            render_batch(
                Rest,
                Channel,
                Valid,
                Results0#{Index => {error, Reason}}
            )
    end.

rendered_to_batch_message(#{subject := Subject, payload := Payload, headers := Headers}) ->
    #{subject => Subject, payload => Payload, headers => Headers}.

results_in_order(Results) ->
    [maps:get(Index, Results) || Index <- lists:seq(1, map_size(Results))].

remove_nth(Index, List) ->
    {Prefix, [_ | Suffix]} = lists:split(Index - 1, List),
    Prefix ++ Suffix.

request_deadline(infinity) -> infinity;
request_deadline(Timeout) -> erlang:monotonic_time(millisecond) + Timeout.

remaining_timeout(infinity) -> infinity;
remaining_timeout(Deadline) -> max(Deadline - erlang:monotonic_time(millisecond), 0).

publish_batch_messages(_Client, _Channel, [], Acc) ->
    {ok, lists:reverse(Acc)};
publish_batch_messages(Client, Channel, [{_ChannelId, Message} | Rest], Acc) ->
    Result = normalize_batch_result(publish_message(Client, Channel, Message, false)),
    case Result of
        ok ->
            publish_batch_messages(Client, Channel, Rest, [ok | Acc]);
        {error, Reason} ->
            case classify_error(Reason) of
                {recoverable_error, _} ->
                    {recoverable, Reason};
                {unrecoverable_error, _} ->
                    publish_batch_messages(Client, Channel, Rest, [Result | Acc])
            end
    end.

normalize_batch_result(ok) ->
    ok;
normalize_batch_result({ok, _Result}) ->
    ok;
normalize_batch_result(Result) ->
    Result.

publish_message(Client, Channel, Message, FlushCore) ->
    maybe
        {ok, Rendered} ?= render_message(Channel, Message),
        Result = publish_rendered(Client, Channel, Rendered),
        maybe_flush_core(Client, Channel, FlushCore, Result)
    else
        {error, _} = Error -> Error
    end.

render_message(Channel, Message) ->
    try
        Subject = emqx_placeholder:proc_tmpl(maps:get(subject, Channel), Message),
        Payload = iolist_to_binary(
            emqx_template:render_strict(
                maps:get(payload, Channel), {emqx_jsonish, Message}
            )
        ),
        Headers = [
            {
                iolist_to_binary(emqx_template:render_strict(Key, {emqx_jsonish, Message})),
                iolist_to_binary(emqx_template:render_strict(Value, {emqx_jsonish, Message}))
            }
         || {Key, Value} <- maps:get(headers, Channel, [])
        ],
        MsgId = render_msg_id(Channel, Message),
        {ok, #{subject => Subject, payload => Payload, headers => Headers, msg_id => MsgId}}
    catch
        Class:Reason -> {error, {template_error, #{class => Class, reason => Reason}}}
    end.

render_msg_id(#{delivery_mode := jetstream, msg_id := MsgIdTemplate}, Message) ->
    iolist_to_binary(emqx_placeholder:proc_tmpl(MsgIdTemplate, Message));
render_msg_id(_Channel, _Message) ->
    undefined.

maybe_flush_core(_Client, _Channel, _FlushCore, {error, _} = Error) ->
    Error;
maybe_flush_core(Client, Channel, true, ok) ->
    enats_client:flush(Client, maps:get(request_ttl, Channel, 5000));
maybe_flush_core(_Client, _Channel, _FlushCore, Result) ->
    Result.

publish_rendered(Client, Channel, #{
    subject := Subject, payload := Payload, headers := Headers, msg_id := MsgId
}) ->
    Options = #{
        headers => Headers,
        timeout => maps:get(request_ttl, Channel, 5000)
    },
    case maps:get(delivery_mode, Channel, core) of
        core ->
            enats_client:publish(Client, Subject, Payload, Options);
        jetstream ->
            case MsgId of
                <<>> ->
                    enats_client:jetstream_publish(Client, Subject, Payload, Options);
                Value ->
                    enats_client:jetstream_publish(Client, Subject, Payload, Options#{
                        msg_id => Value
                    })
            end
    end.

%%--------------------------------------------------------------------
%% Connector configuration and authentication
%%--------------------------------------------------------------------

client_options(Config) ->
    Servers0 = emqx_schema:parse_servers(maps:get(servers, Config), #{default_port => 4222}),
    Servers = [{maps:get(hostname, Server), maps:get(port, Server)} || Server <- Servers0],
    SSL = maps:get(ssl, Config, #{}),
    case auth_options(maps:get(authentication, Config, none)) of
        {error, _} = Error ->
            Error;
        Auth ->
            #{
                servers => Servers,
                connect_timeout => maps:get(connect_timeout, Config, 5000),
                tls => maps:get(enable, SSL, false),
                tls_handshake => maps:get(tls_handshake, Config, starttls),
                ssl_opts => emqx_tls_lib:to_client_opts(SSL),
                auth => Auth,
                reconnect => true,
                notify => false
            }
    end.

auth_options(none) ->
    none;
auth_options(#{mechanism := token, token := Token}) ->
    #{mechanism => token, token => secret_provider(Token)};
auth_options(#{mechanism := user_password, username := Username, password := Password}) ->
    #{mechanism => user_password, username => Username, password => secret_provider(Password)};
auth_options(#{mechanism := nkey, nkey_seed := Seed}) ->
    #{mechanism => nkey_seed, seed => secret_provider(Seed)};
auth_options(#{mechanism := jwt, credentials_file := Filename}) ->
    case enats_auth:validate_credentials_file(Filename) of
        ok ->
            {ok, Auth} = enats_auth:credentials_file(Filename),
            Auth;
        {error, Reason} ->
            {error, Reason}
    end;
auth_options(Authentication) ->
    {error, {invalid_authentication, Authentication}}.

secret_provider(Secret) ->
    fun() -> emqx_secret:unwrap(Secret) end.

%%--------------------------------------------------------------------
%% Result and error classification
%%--------------------------------------------------------------------

classify_result(ok) ->
    ok;
classify_result(Results) when is_list(Results) ->
    [classify_result(Result) || Result <- Results];
classify_result({error, ecpool_empty}) ->
    {error, {recoverable_error, disconnected}};
classify_result({error, Reason}) ->
    {error, classify_error(Reason)};
classify_result(Result) ->
    Result.

classify_error(disconnected) ->
    {recoverable_error, disconnected};
classify_error(reconnecting) ->
    {recoverable_error, reconnecting};
classify_error(closed) ->
    {recoverable_error, closed};
classify_error(stale_connection) ->
    {recoverable_error, stale_connection};
classify_error(timeout) ->
    {recoverable_error, timeout};
classify_error(econnrefused) ->
    {recoverable_error, econnrefused};
classify_error({transport, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({tls_upgrade_failed, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({client_exit, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({auth, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({protocol, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({invalid_batch_message, _Index, _Reason} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({batch_too_large, _Kind, _Actual, _Limit} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({disconnected, {server_error, ServerReason}}) ->
    {unrecoverable_error, {server_error, ServerReason}};
classify_error({disconnected, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({payload_too_large, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error(headers_not_supported = Reason) ->
    {unrecoverable_error, Reason};
classify_error({invalid_subject, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({template_error, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({server_error, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({jetstream_unavailable, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({jetstream_rejected, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({jetstream_error, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({invalid_msg_id, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({jetstream, unavailable, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({jetstream, rejected, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error({jetstream, invalid_ack, _} = Reason) ->
    {unrecoverable_error, Reason};
classify_error(Reason) ->
    {unrecoverable_error, Reason}.
