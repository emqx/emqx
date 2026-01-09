%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_const_v1).

-export([make_sni_fun/1, make_managed_certs_sni_fun/1]).

make_sni_fun(ListenerID) ->
    fun(SN) -> emqx_ocsp_cache:sni_fun(SN, ListenerID) end.

make_managed_certs_sni_fun(PerSNIOpts) ->
    fun(ServerName) ->
        case maps:find(ServerName, PerSNIOpts) of
            {ok, SNIOpts} ->
                SNIOpts;
            error ->
                %% Fallback to default opts
                undefined
        end
    end.
