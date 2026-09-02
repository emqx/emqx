-module(emqx_bridge_nats_connector).

-behaviour(emqx_resource).
-behaviour(ecpool_worker).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
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
-export([connect/1, publish/3]).

resource_type() -> ?CONNECTOR_TYPE.
callback_mode() -> always_sync.

on_start(InstanceId, Config) ->
    ClientOpts = client_options(Config),
    PoolOpts = [
        {pool, InstanceId},
        {pool_size, maps:get(pool_size, Config, 8)},
        {config, ClientOpts}
    ],
    case emqx_resource_pool:start(InstanceId, ?MODULE, PoolOpts) of
        ok -> {ok, #{pool => InstanceId, channels => #{}}};
        {error, Reason} -> {error, Reason}
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
    Workers = ecpool:workers(InstanceId),
    case Workers of
        [] ->
            ?status_disconnected;
        _ ->
            Statuses = [worker_status(Worker) || {_Name, Worker} <- Workers],
            combine_statuses(Statuses)
    end.

on_query(InstanceId, {ChannelId, Message}, #{channels := Channels}) ->
    case maps:find(ChannelId, Channels) of
        {ok, Channel} ->
            Result = ecpool:pick_and_do(
                InstanceId, {?MODULE, publish, [Channel, Message]}, no_handover
            ),
            classify_result(Result);
        error ->
            {error, {unrecoverable_error, {invalid_channel, ChannelId}}}
    end;
on_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

on_batch_query(InstanceId, [{_ChannelId, _Message} | _] = Batch, State) ->
    lists:foldl(
        fun
            ({_ChannelId0, _Message0} = Query, ok) -> on_query(InstanceId, Query, State);
            (_Query, Error) -> Error
        end,
        ok,
        Batch
    );
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
    Options = #{
        headers => Headers,
        timeout => maps:get(request_ttl, Channel, 5000)
    },
    case maps:get(delivery_mode, Channel, core) of
        core ->
            enats_client:publish(Client, Subject, Payload, Options);
        jetstream ->
            MsgId = emqx_placeholder:proc_tmpl(maps:get(msg_id, Channel), Message),
            case iolist_to_binary(MsgId) of
                <<>> ->
                    {error, {invalid_msg_id, missing}};
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
    Auth = auth_options(Config),
    #{
        servers => Servers,
        connect_timeout => maps:get(connect_timeout, Config, 5000),
        tls => maps:get(enable, SSL, false),
        ssl_opts => emqx_tls_lib:to_client_opts(SSL),
        auth => Auth,
        reconnect => true
    }.

auth_options(#{token := Token}) when Token =/= <<>> ->
    #{mechanism => token, token => secret_provider(Token)};
auth_options(#{auth_type := token, token := Token}) ->
    #{mechanism => token, token => secret_provider(Token)};
auth_options(#{auth_type := user_password, username := Username, password := Password}) ->
    #{mechanism => user_password, username => Username, password => secret_provider(Password)};
auth_options(#{auth_type := nkey, public_key := PublicKey, nkey_seed := Seed}) ->
    case enats_nkey:from_seed(emqx_secret:unwrap(Seed)) of
        {ok, PublicKey0, SignFun} when PublicKey =:= <<>>; PublicKey =:= PublicKey0 ->
            #{mechanism => nkey, public_key => PublicKey0, sign_fun => SignFun};
        {ok, _PublicKey0, _SignFun} ->
            error(invalid_nkey_public_key);
        {error, Reason} ->
            error(Reason)
    end;
auth_options(#{auth_type := jwt, public_key := PublicKey, jwt := JWT, nkey_seed := Seed}) ->
    {ok, PublicKey0, SignFun} = enats_nkey:from_seed(emqx_secret:unwrap(Seed)),
    #{
        mechanism => jwt,
        public_key => choose_public_key(PublicKey, PublicKey0),
        jwt => secret_provider(JWT),
        sign_fun => SignFun
    };
auth_options(#{auth_type := creds_file, credentials_file := Filename}) ->
    creds_auth_options(Filename);
auth_options(#{username := Username, password := Password}) when
    Username =/= <<>>, Password =/= <<>>
->
    #{mechanism => user_password, username => Username, password => secret_provider(Password)};
auth_options(_Config) ->
    none.

secret_provider(Secret) ->
    fun() -> emqx_secret:unwrap(Secret) end.

choose_public_key(<<>>, Default) -> Default;
choose_public_key(Value, _Default) -> Value.

creds_auth_options(Filename) ->
    {ok, Contents} = file:read_file(Filename),
    JWT = extract_creds(
        Contents, <<"-----BEGIN NATS USER JWT-----">>, <<"-----END NATS USER JWT-----">>
    ),
    Seed = extract_creds(
        Contents, <<"-----BEGIN USER NKEY SEED-----">>, <<"-----END USER NKEY SEED-----">>
    ),
    {ok, PublicKey, SignFun} = enats_nkey:from_seed(Seed),
    #{mechanism => jwt, public_key => PublicKey, jwt => fun() -> JWT end, sign_fun => SignFun}.

extract_creds(Contents, Begin, End) ->
    case binary:split(Contents, Begin) of
        [_, Rest] ->
            [Value | _] = binary:split(Rest, End),
            trim(Value);
        _ ->
            error({invalid_credentials_file, Begin})
    end.

trim(Value) ->
    iolist_to_binary(string:trim(binary_to_list(Value))).

worker_status(Worker) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} ->
            case enats_client:status(Client) of
                connected ->
                    case enats_client:flush(Client, 1000) of
                        ok -> connected;
                        {error, flush_in_progress} -> connecting;
                        {error, _} -> disconnected
                    end;
                connecting ->
                    connecting;
                reconnecting ->
                    connecting;
                disconnected ->
                    disconnected
            end;
        {error, _} ->
            disconnected
    end.

combine_statuses(Statuses) ->
    case lists:member(disconnected, Statuses) of
        true ->
            ?status_disconnected;
        false ->
            case lists:member(connecting, Statuses) of
                true -> ?status_connecting;
                false -> ?status_connected
            end
    end.

classify_result(ok) -> ok;
classify_result({error, ecpool_empty}) -> {error, {recoverable_error, disconnected}};
classify_result({error, Reason}) -> {error, classify_error(Reason)};
classify_result(Result) -> Result.

classify_error(disconnected) -> {recoverable_error, disconnected};
classify_error(reconnecting) -> {recoverable_error, reconnecting};
classify_error(closed) -> {recoverable_error, closed};
classify_error(timeout) -> {recoverable_error, timeout};
classify_error(econnrefused) -> {recoverable_error, econnrefused};
classify_error({disconnected, _}) -> {recoverable_error, disconnected};
classify_error({payload_too_large, _} = Reason) -> {unrecoverable_error, Reason};
classify_error(headers_not_supported = Reason) -> {unrecoverable_error, Reason};
classify_error({invalid_subject, _} = Reason) -> {unrecoverable_error, Reason};
classify_error({server_error, _} = Reason) -> {unrecoverable_error, Reason};
classify_error({jetstream_error, _} = Reason) -> {unrecoverable_error, Reason};
classify_error({invalid_msg_id, _} = Reason) -> {unrecoverable_error, Reason};
classify_error(Reason) -> {unrecoverable_error, Reason}.
