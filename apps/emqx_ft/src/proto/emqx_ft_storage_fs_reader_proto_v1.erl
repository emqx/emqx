%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_storage_fs_reader_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([read/3]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.17".

-spec read(node(), pid(), pos_integer()) ->
    {ok, binary()} | eof | {error, term()} | no_return().
read(Node, Pid, Bytes) when
    is_atom(Node) andalso is_pid(Pid) andalso is_integer(Bytes) andalso Bytes > 0
->
    emqx_rpc:call(Node, emqx_ft_storage_fs_reader, read, [Pid, Bytes]).
