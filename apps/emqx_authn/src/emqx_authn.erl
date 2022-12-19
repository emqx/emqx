%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn).

-export([
    providers/0,
    check_config/1,
    check_config/2,
    check_configs/1,
    %% for telemetry information
    get_enabled_authns/0
]).

-include("emqx_authn.hrl").

providers() ->
    [
        {{password_based, built_in_database}, emqx_authn_mnesia},
        {{password_based, mysql}, emqx_authn_mysql},
        {{password_based, postgresql}, emqx_authn_pgsql},
        {{password_based, mongodb}, emqx_authn_mongodb},
        {{password_based, redis}, emqx_authn_redis},
        {{password_based, http}, emqx_authn_http},
        {jwt, emqx_authn_jwt},
        {{scram, built_in_database}, emqx_enhanced_authn_scram_mnesia}
    ].

check_configs(CM) when is_map(CM) ->
    check_configs([CM]);
check_configs(CL) ->
    check_configs(CL, 1).

check_configs([], _Nth) ->
    [];
check_configs([Config | Configs], Nth) ->
    [check_config(Config, #{id_for_log => Nth}) | check_configs(Configs, Nth + 1)].

check_config(Config) ->
    check_config(Config, #{}).

check_config(Config, Opts) ->
    case do_check_config(Config, Opts) of
        #{?CONF_NS_ATOM := Checked} -> Checked;
        #{?CONF_NS_BINARY := WithDefaults} -> WithDefaults
    end.

do_check_config(#{<<"mechanism">> := Mec0} = Config, Opts) ->
    Mec = atom(Mec0, #{error => unknown_mechanism}),
    Key =
        case maps:get(<<"backend">>, Config, false) of
            false -> Mec;
            Backend -> {Mec, atom(Backend, #{error => unknown_backend})}
        end,
    case lists:keyfind(Key, 1, providers()) of
        false ->
            throw(#{error => unknown_authn_provider, which => Key});
        {_, ProviderModule} ->
            hocon_tconf:check_plain(
                ProviderModule,
                #{?CONF_NS_BINARY => Config},
                Opts#{atom_key => true}
            )
    end;
do_check_config(Config, Opts) when is_map(Config) ->
    throw(#{
        error => invalid_config,
        which => maps:get(id_for_log, Opts, unknown),
        reason => "mechanism_field_required"
    }).

%% The atoms have to be loaded already,
%% which might be an issue for plugins which are loaded after node boot
%% but they should really manage their own configs in that case.
atom(Bin, ErrorContext) ->
    try
        binary_to_existing_atom(Bin, utf8)
    catch
        _:_ ->
            throw(ErrorContext#{value => Bin})
    end.

-spec get_enabled_authns() ->
    #{
        authenticators => [authenticator_id()],
        overridden_listeners => #{authenticator_id() => pos_integer()}
    }.
get_enabled_authns() ->
    %% at the moment of writing, `emqx_authentication:list_chains/0'
    %% result is always wrapped in `{ok, _}', and it cannot return any
    %% error values.
    {ok, Chains} = emqx_authentication:list_chains(),
    AuthnTypes = lists:usort([
        Type
     || #{authenticators := As} <- Chains,
        #{id := Type} <- As
    ]),
    OverriddenListeners =
        lists:foldl(
            fun
                (#{name := ?GLOBAL}, Acc) ->
                    Acc;
                (#{authenticators := As}, Acc) ->
                    lists:foldl(fun tally_authenticators/2, Acc, As)
            end,
            #{},
            Chains
        ),
    #{
        authenticators => AuthnTypes,
        overridden_listeners => OverriddenListeners
    }.

tally_authenticators(#{id := AuthenticatorName}, Acc) ->
    maps:update_with(AuthenticatorName, fun(N) -> N + 1 end, 1, Acc).
