%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_nif).

-moduledoc """
NIF stubs of the EMQX native library. Do not call this module directly. Each API has a wrapper
module, for example `emqx_niftls`.
""".

-export([
    info/0,
    tls_config/1,
    tls_new/1,
    tls_feed_handshake/2,
    tls_feed/2,
    tls_encrypt/2,
    tls_close/1
]).

-nifs([
    info/0,
    tls_config/1,
    tls_new/1,
    tls_feed_handshake/2,
    tls_feed/2,
    tls_encrypt/2,
    tls_close/1
]).

-on_load(init/0).

init() ->
    Lib = filename:join(code:priv_dir(emqx_nif), "libemqx_nif"),
    erlang:load_nif(Lib, 0).

-define(NOT_LOADED, erlang:nif_error(nif_not_loaded)).

-doc "Return the global counters of the native side.".
-spec info() -> #{atom() => integer()}.
info() -> ?NOT_LOADED.

tls_config(_Opts) -> ?NOT_LOADED.
tls_new(_Conf) -> ?NOT_LOADED.
tls_feed_handshake(_Tls, _Bin) -> ?NOT_LOADED.
tls_feed(_Tls, _Bin) -> ?NOT_LOADED.
tls_encrypt(_Tls, _Data) -> ?NOT_LOADED.
tls_close(_Tls) -> ?NOT_LOADED.
