%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_provider).

-export([
    create/1,
    delete/1,
    update/2,
    hackney_pool/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create(emqx_ai_completion_config:provider()) -> ok | {error, term()}.
create(#{transport_options := #{max_connections := MaxConnections}} = Provider) ->
    hackney_pool:start_pool(
        hackney_pool(Provider),
        [
            {max_connections, MaxConnections}
        ]
    ).

-spec delete(emqx_ai_completion_config:provider()) -> ok.
delete(Provider) ->
    _ = hackney_pool:stop_pool(hackney_pool(Provider)),
    ok.

-spec update(emqx_ai_completion_config:provider(), emqx_ai_completion_config:provider()) ->
    ok | {error, term()}.
update(
    #{transport_options := #{max_connections := MaxConnectionsOld}} = OldProvider,
    #{transport_options := #{max_connections := MaxConnectionsNew}} = _NewProvider
) ->
    case MaxConnectionsOld =:= MaxConnectionsNew of
        true ->
            ok;
        false ->
            hackney_pool:set_max_connections(hackney_pool(OldProvider), MaxConnectionsNew)
    end.

-spec hackney_pool(emqx_ai_completion_config:provider()) -> term().
hackney_pool(#{name := Name} = _Provider) ->
    {?MODULE, Name}.
