-module(emqx_acme_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1,
    on_config_changed/2,
    on_handle_api_call/4
]).

start(_StartType, _StartArgs) ->
    ok = emqx_acme_config:load(),
    emqx_acme_sup:start_link().

stop(_State) ->
    emqx_acme_challenge:stop(),
    ok.

on_config_changed(_OldConfig, NewConfig) ->
    case emqx_acme_config:update(NewConfig) of
        ok ->
            emqx_acme_issuer:reconfigure(),
            ok;
        {error, _} = Error ->
            Error
    end.

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_acme_api:handle(Method, PathRemainder, Request).
