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
        {emqx_template:parse(maps:get(key, Header)), emqx_template:parse(maps:get(value, Header))}
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

on_query(InstanceId, {ChannelId, Message}, #{channels := Channels}) ->
    case maps:find(ChannelId, Channels) of
        {ok, Channel} ->
            RawResult = ecpool:pick_and_do(
                InstanceId, {?MODULE, publish, [Channel, Message]}, no_handover
            ),
            Result = classify_result(RawResult),
            ?tp(nats_connector_query_return, #{instance_id => InstanceId, result => Result}),
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
    case publish_batch_messages(Client, Channel, Batch, [], false) of
        {recoverable, Reason} ->
            {error, Reason};
        {ok, Results, false} ->
            Results;
        {ok, Results, true} ->
            case maps:get(delivery_mode, Channel, core) of
                core ->
                    case enats_client:flush(Client, maps:get(request_ttl, Channel, 5000)) of
                        ok -> Results;
                        {error, _} = Error -> Error
                    end;
                jetstream ->
                    Results
            end
    end.

publish_batch_messages(_Client, _Channel, [], Acc, Sent) ->
    {ok, lists:reverse(Acc), Sent};
publish_batch_messages(Client, Channel, [{_ChannelId, Message} | Rest], Acc, Sent) ->
    Result = normalize_batch_result(publish_message(Client, Channel, Message, false)),
    case Result of
        ok ->
            publish_batch_messages(Client, Channel, Rest, [ok | Acc], true);
        {error, Reason} ->
            case classify_error(Reason) of
                {recoverable_error, _} ->
                    {recoverable, Reason};
                {unrecoverable_error, _} ->
                    publish_batch_messages(Client, Channel, Rest, [Result | Acc], Sent)
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
    case enats_credentials:validate_file(Filename) of
        ok ->
            {ok, Auth} = enats_credentials:from_file(Filename),
            Auth;
        {error, Reason} ->
            {error, Reason}
    end;
auth_options(Authentication) ->
    {error, {invalid_authentication, Authentication}}.

secret_provider(Secret) ->
    fun() -> emqx_secret:unwrap(Secret) end.

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
classify_error(timeout) ->
    {recoverable_error, timeout};
classify_error(econnrefused) ->
    {recoverable_error, econnrefused};
classify_error({disconnected, {server_error, ServerReason}}) ->
    {unrecoverable_error, {server_error, ServerReason}};
classify_error({disconnected, _}) ->
    {recoverable_error, disconnected};
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
classify_error(Reason) ->
    {unrecoverable_error, Reason}.
