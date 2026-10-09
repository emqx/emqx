%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_subject).

-export([tokens/1, matches/2, is_subset/2, intersects/2]).

-type tokens() :: [binary()].

-spec tokens(binary()) -> tokens().
tokens(Subject) ->
    case emqx_nats_topic:validate_nats_subject(Subject) of
        {ok, _} -> binary:split(Subject, <<".">>, [global]);
        {error, Reason} -> error({invalid_subject, Reason})
    end.

-spec matches(tokens(), tokens()) -> boolean().
matches([], []) ->
    true;
matches([_ | _], [<<">">>]) ->
    true;
matches([_ | Subject], [<<"*">> | Filter]) ->
    matches(Subject, Filter);
matches([Token | Subject], [Token | Filter]) ->
    matches(Subject, Filter);
matches(_, _) ->
    false.

-spec is_subset(tokens(), tokens()) -> boolean().
is_subset([], []) ->
    true;
is_subset([_ | _], [<<">">>]) ->
    true;
is_subset([<<">">>], _) ->
    false;
is_subset([_ | Subject], [<<"*">> | Filter]) ->
    is_subset(Subject, Filter);
is_subset([Token | Subject], [Token | Filter]) ->
    is_subset(Subject, Filter);
is_subset(_, _) ->
    false.

-spec intersects(tokens(), tokens()) -> boolean().
intersects([], []) ->
    true;
intersects([_ | _], [<<">">>]) ->
    true;
intersects([<<">">>], [_ | _]) ->
    true;
intersects([_ | Subject], [<<"*">> | Filter]) ->
    intersects(Subject, Filter);
intersects([<<"*">> | Subject], [_ | Filter]) ->
    intersects(Subject, Filter);
intersects([Token | Subject], [Token | Filter]) ->
    intersects(Subject, Filter);
intersects(_, _) ->
    false.
