%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_listener_config).

-behaviour(emqx_config_handler).
-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([start_link/0]).

%% emqx_config_handler API
-export([add_handler/0, remove_handler/0]).
-export([pre_config_update/3, post_config_update/5]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%%--------------------------------------------------------------------
%% gen_server messages
%%--------------------------------------------------------------------

-record(update_listeners, {
    old_listeners :: emqx_dashboard:listener_configs(),
    new_listeners :: emqx_dashboard:listener_configs()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = add_handler(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, not_implemented}, State, hibernate}.

handle_cast(#update_listeners{old_listeners = OldListeners, new_listeners = NewListeners}, State) ->
    ok = wait_for_config_update(),
    ok = emqx_dashboard:stop_listeners(OldListeners),
    ok = emqx_dashboard:start_listeners(NewListeners),
    {noreply, State, hibernate};
handle_cast(_Request, State) ->
    {noreply, State, hibernate}.

handle_info(_Info, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok = remove_handler(),
    ok.

%%--------------------------------------------------------------------
%% emqx_config_handler API
%%--------------------------------------------------------------------

add_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:add_handler(Roots, ?MODULE),
    ok.

remove_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:remove_handler(Roots),
    ok.

pre_config_update(_Path, {change_i18n_lang, NewLang}, RawConf) ->
    %% e.g. emqx_conf:update([dashboard], {change_i18n_lang, zh}, #{}).
    %% TODO: check if there is such a language (all languages are cached in emqx_dashboard_desc_cache)
    Update = #{<<"i18n_lang">> => NewLang},
    NewConf = emqx_utils_maps:deep_merge(RawConf, Update),
    {ok, NewConf};
pre_config_update(_Path, UpdateConf0, RawConf) ->
    UpdateConf = remove_sensitive_data(UpdateConf0),
    NewConf = emqx_utils_maps:deep_merge(RawConf, UpdateConf),
    ensure_ssl_cert(NewConf).

post_config_update(_, {change_i18n_lang, _}, _NewConf, _OldConf, _AppEnvs) ->
    emqx_dashboard:regenerate_dispatch_after_config_update();
post_config_update(_, _Req, NewConf, OldConf, _AppEnvs) ->
    SwaggerSupport = diff_swagger_support(NewConf, OldConf),
    OldHttp = get_listener(http, OldConf),
    OldHttps = get_listener(https, OldConf),
    NewHttp = get_listener(http, NewConf),
    NewHttps = get_listener(https, NewConf),
    {StopHttp, StartHttp} = diff_listeners(http, OldHttp, NewHttp, SwaggerSupport),
    {StopHttps, StartHttps} = diff_listeners(https, OldHttps, NewHttps, SwaggerSupport),
    Stop = maps:merge(StopHttp, StopHttps),
    Start = maps:merge(StartHttp, StartHttps),
    update_listeners_after_config_update(Stop, Start).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

update_listeners_after_config_update(Stop, Start) ->
    gen_server:cast(?MODULE, #update_listeners{old_listeners = Stop, new_listeners = Start}).

-define(SENSITIVE_PASSWORD, <<"******">>).

remove_sensitive_data(Conf0) ->
    Conf1 =
        case Conf0 of
            #{<<"default_password">> := ?SENSITIVE_PASSWORD} ->
                maps:remove(<<"default_password">>, Conf0);
            _ ->
                Conf0
        end,
    case Conf1 of
        #{<<"listeners">> := #{<<"https">> := #{<<"password">> := ?SENSITIVE_PASSWORD}}} ->
            emqx_utils_maps:deep_remove([<<"listeners">>, <<"https">>, <<"password">>], Conf1);
        _ ->
            Conf1
    end.

get_listener(Type, Conf) ->
    emqx_utils_maps:deep_get([listeners, Type], Conf, undefined).

diff_swagger_support(NewConf, OldConf) ->
    maps:get(swagger_support, NewConf, true) =:=
        maps:get(swagger_support, OldConf, true).

diff_listeners(_, undefined, undefined, _) -> {#{}, #{}};
diff_listeners(_, Listener, Listener, true) -> {#{}, #{}};
diff_listeners(Type, undefined, Start, _) -> {#{}, #{Type => Start}};
diff_listeners(Type, Stop, undefined, _) -> {#{Type => Stop}, #{}};
diff_listeners(Type, Listener, Listener, false) -> {#{Type => Listener}, #{Type => Listener}};
diff_listeners(Type, Stop, Start, _) -> {#{Type => Stop}, #{Type => Start}}.

-define(DIR, <<"dashboard">>).

ensure_ssl_cert(#{<<"listeners">> := #{<<"https">> := #{<<"bind">> := Bind} = Https0}} = Conf0) when
    Bind =/= 0
->
    Https1 = emqx_dashboard_schema:https_converter(Https0, #{}),
    Conf1 = emqx_utils_maps:deep_put([<<"listeners">>, <<"https">>], Conf0, Https1),
    Ssl = maps:get(<<"ssl_options">>, Https1, undefined),
    Opts = #{required_keys => [[<<"keyfile">>], [<<"certfile">>]]},
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(?DIR, Ssl, Opts) of
        {ok, undefined} ->
            {error, <<"ssl_cert_not_found">>};
        {ok, NewSsl} ->
            Keys = [<<"listeners">>, <<"https">>, <<"ssl_options">>],
            {ok, emqx_utils_maps:deep_put(Keys, Conf1, NewSsl)};
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            {error, Reason}
    end;
ensure_ssl_cert(Conf) ->
    {ok, Conf}.

%% NOTE: some requests for dashboard update may be issued from pre/post_config_update hooks.
%% We should wait for the config update to be completed before performing the requested updates.
%% We do this by calling emqx_config_handler's gen_server which performs pre/post_config_update hooks.
%% Having got a reply from the gen_server, we can be sure that the config update is completed.
wait_for_config_update() ->
    _ = emqx_config_handler:info(),
    ok.
