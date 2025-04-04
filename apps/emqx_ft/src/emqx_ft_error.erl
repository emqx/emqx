%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc File Transfer error description module

-module(emqx_ft_error).

-export([format/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format(ok) -> <<"success">>;
format({error, Reason}) -> format_error_reson(Reason).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

format_error_reson(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
format_error_reson({ErrorKind, _}) when is_atom(ErrorKind) ->
    atom_to_binary(ErrorKind, utf8);
format_error_reson(_Reason) ->
    <<"internal_error">>.
