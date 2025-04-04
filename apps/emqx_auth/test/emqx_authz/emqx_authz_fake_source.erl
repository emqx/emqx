%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_fake_source).

-behaviour(emqx_authz_source).

%% APIs
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

description() ->
    "Fake AuthZ".

create(Source) ->
    Source.

update(Source) ->
    Source.

destroy(_Source) -> ok.

authorize(_Client, _PubSub, _Topic, _Source) ->
    nomatch.
