%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_profile_conf).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_s3.hrl").

-export([
    start_link/2,
    child_spec/2
]).

-export([
    checkout_config/1,
    checkout_config/2,
    checkin_config/1,
    checkin_config/2,

    update_config/2,
    update_config/3
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% For connectors
-export([
    client_config/2
]).

%% For test purposes
-export([
    start_http_pool/2,
    id/1
]).

-type config_checkout() :: {
    emqx_s3_client:bucket(),
    emqx_s3_client:config(),
    emqx_s3_client:upload_options(),
    emqx_s3_upload:config()
}.

-define(DEFAULT_CALL_TIMEOUT, 5000).

-define(SAFE_CALL_VIA_GPROC(ProfileId, Message, Timeout),
    ?SAFE_CALL_VIA_GPROC(id(ProfileId), Message, Timeout, profile_not_found)
).

-spec child_spec(emqx_s3:profile_id(), emqx_s3:profile_config()) -> supervisor:child_spec().
child_spec(ProfileId, ProfileConfig) ->
    #{
        id => ProfileId,
        start => {?MODULE, start_link, [ProfileId, ProfileConfig]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec start_link(emqx_s3:profile_id(), emqx_s3:profile_config()) -> gen_server:start_ret().
start_link(ProfileId, ProfileConfig) ->
    gen_server:start_link(?VIA_GPROC(id(ProfileId)), ?MODULE, [ProfileId, ProfileConfig], []).

-spec update_config(emqx_s3:profile_id(), emqx_s3:profile_config()) -> ok_or_error(term()).
update_config(ProfileId, ProfileConfig) ->
    update_config(ProfileId, ProfileConfig, ?DEFAULT_CALL_TIMEOUT).

-spec update_config(emqx_s3:profile_id(), emqx_s3:profile_config(), timeout()) ->
    ok_or_error(term()).
update_config(ProfileId, ProfileConfig, Timeout) ->
    ?SAFE_CALL_VIA_GPROC(ProfileId, {update_config, ProfileConfig}, Timeout).

-spec checkout_config(emqx_s3:profile_id()) ->
    config_checkout() | {error, profile_not_found}.
checkout_config(ProfileId) ->
    checkout_config(ProfileId, ?DEFAULT_CALL_TIMEOUT).

-spec checkout_config(emqx_s3:profile_id(), timeout()) ->
    config_checkout() | {error, profile_not_found}.
checkout_config(ProfileId, Timeout) ->
    ?SAFE_CALL_VIA_GPROC(ProfileId, {checkout_config, self()}, Timeout).

-spec checkin_config(emqx_s3:profile_id()) -> ok | {error, profile_not_found}.
checkin_config(ProfileId) ->
    checkin_config(ProfileId, ?DEFAULT_CALL_TIMEOUT).

-spec checkin_config(emqx_s3:profile_id(), timeout()) -> ok | {error, profile_not_found}.
checkin_config(ProfileId, Timeout) ->
    ?SAFE_CALL_VIA_GPROC(ProfileId, {checkin_config, self()}, Timeout).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([ProfileId, ProfileConfig]) ->
    _ = process_flag(trap_exit, true),
    ok = cleanup_profile_pools(ProfileId),
    case start_http_pool(ProfileId, ProfileConfig) of
        {ok, PoolName} ->
            {ok, #{
                profile_id => ProfileId,
                profile_config => ProfileConfig,
                bucket => bucket(ProfileConfig),
                upload_options => upload_options(ProfileConfig),
                client_config => client_config(ProfileConfig, PoolName),
                uploader_config => uploader_config(ProfileConfig),
                pool_name => PoolName
            }};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(
    {checkout_config, Pid},
    _From,
    #{
        bucket := Bucket,
        upload_options := Options,
        client_config := ClientConfig,
        uploader_config := UploaderConfig
    } = State
) ->
    ok = register_client(Pid, State),
    {reply, {Bucket, ClientConfig, Options, UploaderConfig}, State};
handle_call({checkin_config, Pid}, _From, State) ->
    ok = unregister_client(Pid, State),
    {reply, ok, State};
handle_call(
    {update_config, NewProfileConfig},
    _From,
    #{profile_id := ProfileId} = State
) ->
    case update_http_pool(ProfileId, NewProfileConfig, State) of
        {ok, PoolName} ->
            NewState = State#{
                profile_config => NewProfileConfig,
                bucket => bucket(NewProfileConfig),
                upload_options => upload_options(NewProfileConfig),
                client_config => client_config(NewProfileConfig, PoolName),
                uploader_config => uploader_config(NewProfileConfig),
                pool_name => PoolName
            },
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{profile_id := ProfileId}) ->
    cleanup_profile_pools(ProfileId).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

id(ProfileId) ->
    {?MODULE, ProfileId}.

client_config(ProfileConfig, PoolName) ->
    HTTPOpts = maps:get(transport_options, ProfileConfig, #{}),
    #{
        scheme => scheme(HTTPOpts),
        host => maps:get(host, ProfileConfig),
        port => maps:get(port, ProfileConfig),
        access_method => maps:get(access_method, ProfileConfig, path),
        url_expire_time => maps:get(url_expire_time, ProfileConfig),
        headers => maps:get(headers, HTTPOpts, #{}),
        access_key_id => maps:get(access_key_id, ProfileConfig, undefined),
        secret_access_key => maps:get(secret_access_key, ProfileConfig, undefined),
        http_client => emqx_s3_client_http:new(PoolName, ProfileConfig),
        max_retries => maps:get(max_retries, HTTPOpts, undefined)
    }.

uploader_config(#{max_part_size := MaxPartSize, min_part_size := MinPartSize} = _ProfileConfig) ->
    #{
        min_part_size => MinPartSize,
        max_part_size => MaxPartSize
    }.

bucket(ProfileConfig) ->
    maps:get(bucket, ProfileConfig).

upload_options(ProfileConfig) ->
    #{acl => maps:get(acl, ProfileConfig, undefined)}.

scheme(#{ssl := #{enable := true}}) -> "https://";
scheme(_TransportOpts) -> "http://".

start_http_pool(ProfileId, ProfileConfig) ->
    PoolName = pool_name(ProfileId),
    case emqx_s3_client_http:start_pool(PoolName, ProfileConfig) of
        ok ->
            ok = emqx_s3_profile_http_pools:register(ProfileId, PoolName),
            ok = ?tp(debug, "s3_start_http_pool", #{pool_name => PoolName, profile_id => ProfileId}),
            {ok, PoolName};
        {error, _} = Error ->
            Error
    end.

update_http_pool(_ProfileId, ProfileConfig, #{pool_name := PoolName}) ->
    case emqx_s3_client_http:update_pool(PoolName, ProfileConfig) of
        ok ->
            {ok, PoolName};
        {error, _} = Error ->
            Error
    end.

pool_name(ProfileId) ->
    iolist_to_binary([<<"s3-http-">>, profile_id_to_bin(ProfileId)]).

profile_id_to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
profile_id_to_bin(Bin) when is_binary(Bin) -> Bin.

cleanup_profile_pools(ProfileId) ->
    lists:foreach(
        fun(PoolName) ->
            ok = stop_http_pool(ProfileId, PoolName)
        end,
        emqx_s3_profile_http_pools:all(ProfileId)
    ).

register_client(_Pid, #{profile_id := ProfileId, pool_name := PoolName}) ->
    _ = emqx_s3_profile_http_pools:register_client(ProfileId, PoolName),
    ok.

unregister_client(_Pid, #{profile_id := ProfileId, pool_name := PoolName}) ->
    _ = emqx_s3_profile_http_pools:unregister_client(ProfileId, PoolName),
    ok.

%%--------------------------------------------------------------------
%% HTTP Pool implementation dependent functions
%%--------------------------------------------------------------------

stop_http_pool(ProfileId, PoolName) ->
    ok = emqx_s3_client_http:stop_pool(PoolName),
    ok = emqx_s3_profile_http_pools:unregister(ProfileId, PoolName),
    ok = ?tp(debug, "s3_stop_http_pool", #{pool_name => PoolName}).
