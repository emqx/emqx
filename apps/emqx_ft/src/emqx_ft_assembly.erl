%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_assembly).

-export([new/1]).
-export([append/3]).
-export([update/1]).

-export([status/1]).
-export([filemeta/1]).
-export([coverage/1]).
-export([properties/1]).

-export_type([t/0]).

-type filemeta() :: emqx_ft:filemeta().
-type filefrag() :: emqx_ft_storage_fs:filefrag().
-type filefrag(T) :: emqx_ft_storage_fs:filefrag(T).
-type segmentinfo() :: emqx_ft_storage_fs:segmentinfo().

-record(asm, {
    status :: status(),
    coverage :: coverage() | undefined,
    properties :: properties() | undefined,
    meta :: orddict:orddict(
        filemeta(),
        {node(), filefrag({filemeta, filemeta()})}
    ),
    segs :: orddict:orddict(
        {emqx_ft:offset(), _Locality, _MEnd, node()},
        filefrag({segment, segmentinfo()})
    ),
    size :: emqx_ft:bytes()
}).

-type status() ::
    {incomplete, {missing, _}}
    | complete
    | {error, {inconsistent, _}}.

-type coverage() :: [{node(), filefrag({segment, segmentinfo()})}].

-type properties() :: #{
    %% Node where "most" of the segments are located.
    dominant => node()
}.

-opaque t() :: #asm{}.

-spec new(emqx_ft:bytes()) -> t().
new(Size) ->
    #asm{
        status = {incomplete, {missing, filemeta}},
        meta = orddict:new(),
        segs = orddict:new(),
        size = Size
    }.

-spec append(t(), node(), filefrag() | [filefrag()]) -> t().
append(Asm, Node, Fragments) when is_list(Fragments) ->
    lists:foldl(fun(F, AsmIn) -> append(AsmIn, Node, F) end, Asm, Fragments);
append(Asm, Node, Fragment = #{fragment := {filemeta, _}}) ->
    append_filemeta(Asm, Node, Fragment);
append(Asm, Node, Segment = #{fragment := {segment, _}}) ->
    append_segmentinfo(Asm, Node, Segment).

-spec update(t()) -> t().
update(Asm) ->
    case status(meta, Asm) of
        {complete, _Meta} ->
            case status(coverage, Asm) of
                {complete, Coverage, Props} ->
                    Asm#asm{
                        status = complete,
                        coverage = Coverage,
                        properties = Props
                    };
                Status ->
                    Asm#asm{status = Status}
            end;
        Status ->
            Asm#asm{status = Status}
    end.

-spec status(t()) -> status().
status(#asm{status = Status}) ->
    Status.

-spec filemeta(t()) -> filemeta().
filemeta(Asm) ->
    case status(meta, Asm) of
        {complete, Meta} -> Meta;
        _Other -> undefined
    end.

-spec coverage(t()) -> coverage() | undefined.
coverage(#asm{coverage = Coverage}) ->
    Coverage.

properties(#asm{properties = Properties}) ->
    Properties.

status(meta, #asm{meta = Meta}) ->
    status(meta, orddict:to_list(Meta));
status(meta, [{Meta, {_Node, _Frag}}]) ->
    {complete, Meta};
status(meta, []) ->
    {incomplete, {missing, filemeta}};
status(meta, [_M1, _M2 | _] = Metas) ->
    {error, {inconsistent, [Frag#{node => Node} || {_, {Node, Frag}} <- Metas]}};
status(coverage, #asm{segs = Segments, size = Size}) ->
    case coverage(orddict:to_list(Segments), 0, Size) of
        Coverage when is_list(Coverage) ->
            {complete, Coverage, #{
                dominant => dominant(Coverage)
            }};
        Missing = {missing, _} ->
            {incomplete, Missing}
    end.

append_filemeta(Asm, Node, Fragment = #{fragment := {filemeta, Meta}}) ->
    Asm#asm{
        meta = orddict:store(Meta, {Node, Fragment}, Asm#asm.meta)
    }.

append_segmentinfo(Asm, _Node, #{fragment := {segment, #{size := 0}}}) ->
    % NOTE
    % Empty segments are valid but meaningless for coverage.
    Asm;
append_segmentinfo(Asm, Node, Fragment = #{fragment := {segment, Info}}) ->
    Offset = maps:get(offset, Info),
    Size = maps:get(size, Info),
    End = Offset + Size,
    Asm#asm{
        % TODO
        % In theory it's possible to have two segments with same offset + size on
        % different nodes but with differing content. We'd need a checksum to
        % be able to disambiguate them though.
        segs = orddict:store({Offset, locality(Node), -End, Node}, Fragment, Asm#asm.segs)
    }.

coverage([{{Offset, _, _, _}, _Segment} | Rest], Cursor, Sz) when Offset < Cursor ->
    coverage(Rest, Cursor, Sz);
coverage([{{Cursor, _Locality, MEnd, Node}, Segment} | _Rest] = Segments, Cursor, Sz) ->
    % NOTE
    % We consider only whole fragments here, so for example from the point of view of
    % this algo `[{Offset1 = 0, Size1 = 15}, {Offset2 = 10, Size2 = 10}]` has no
    % coverage.
    Tail = tail(Segments),
    case coverage(Tail, -MEnd, Sz) of
        Coverage when is_list(Coverage) ->
            [{Node, Segment} | Coverage];
        Missing = {missing, _} ->
            case coverage(Tail, Cursor, Sz) of
                CoverageAlt when is_list(CoverageAlt) ->
                    CoverageAlt;
                {missing, _} ->
                    Missing
            end
    end;
coverage([{{Offset, _MEnd, _, _}, _Segment} | _], Cursor, _Sz) when Offset > Cursor ->
    {missing, {segment, Cursor, Offset}};
coverage([], Cursor, Sz) when Cursor < Sz ->
    {missing, {segment, Cursor, Sz}};
coverage([], Cursor, Cursor) ->
    [].

tail([Segment | Rest]) ->
    tail(Segment, Rest).

tail({{Cursor, _, MEnd, _}, _} = Segment, [{{Cursor, _, MEnd, _}, _} | Rest]) ->
    % NOTE
    % Discarding segments with same offset / size, potentially located on other nodes.
    % This is an optimization. They won't participate in coverage anyway given we're
    % currently optimizing coverage towards locality. Yet if we instead decide to
    % optimize for node dominance (e.g. compute such coverage that most of the data
    % located on a single node) we'll need to account them again.
    tail(Segment, Rest);
tail(_Segment, Tail) ->
    Tail.

dominant(Coverage) ->
    % TODO: needs improvement, better defined _dominance_, maybe some score
    Freqs = frequencies(fun({Node, Segment}) -> {Node, segsize(Segment)} end, Coverage),
    maxfreq(Freqs, node()).

frequencies(Fun, List) ->
    lists:foldl(
        fun(E, Acc) ->
            {K, N} = Fun(E),
            maps:update_with(K, fun(M) -> M + N end, N, Acc)
        end,
        #{},
        List
    ).

maxfreq(Freqs, Init) ->
    {_, Max} = maps:fold(
        fun
            (F, N, {M, _MF}) when N > M -> {N, F};
            (_F, _N, {M, MF}) -> {M, MF}
        end,
        {0, Init},
        Freqs
    ),
    Max.

locality(Node) when Node =:= node() ->
    % NOTE
    % This should prioritize locally available segments over those on remote nodes.
    0;
locality(_RemoteNode) ->
    1.

segsize(#{fragment := {segment, Info}}) ->
    maps:get(size, Info).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

incomplete_new_test() ->
    ?assertEqual(
        {incomplete, {missing, filemeta}},
        status(update(new(42)))
    ).

incomplete_test() ->
    ?assertEqual(
        {incomplete, {missing, filemeta}},
        status(
            update(
                append(new(142), node(), [
                    segment(p1, 0, 42),
                    segment(p1, 42, 100)
                ])
            )
        )
    ).

consistent_test() ->
    Asm1 = append(new(42), n1, [filemeta(m1, "blarg")]),
    Asm2 = append(Asm1, n2, [segment(s2, 0, 42)]),
    Asm3 = append(Asm2, n3, [filemeta(m3, "blarg")]),
    ?assertMatch({complete, _}, status(meta, Asm3)).

inconsistent_test() ->
    Asm1 = append(new(42), node(), [segment(s1, 0, 42)]),
    Asm2 = append(Asm1, n1, [filemeta(m1, "blarg")]),
    Asm3 = append(Asm2, n2, [segment(s2, 0, 42), filemeta(m1, "blorg")]),
    Asm4 = append(Asm3, n3, [filemeta(m3, "blarg")]),
    ?assertMatch(
        {error,
            {inconsistent, [
                % blarg < blorg
                #{node := n3, path := m3, fragment := {filemeta, #{name := "blarg"}}},
                #{node := n2, path := m1, fragment := {filemeta, #{name := "blorg"}}}
            ]}},
        status(meta, Asm4)
    ).

simple_coverage_test() ->
    Node = node(),
    Segs = [
        {node42, segment(n1, 20, 30)},
        {Node, segment(n2, 0, 10)},
        {Node, segment(n3, 50, 50)},
        {Node, segment(n4, 10, 10)}
    ],
    Asm = append_many(new(100), Segs),
    ?assertMatch(
        {complete,
            [
                {Node, #{path := n2}},
                {Node, #{path := n4}},
                {node42, #{path := n1}},
                {Node, #{path := n3}}
            ],
            #{dominant := Node}},
        status(coverage, Asm)
    ).

redundant_coverage_test() ->
    Node = node(),
    Segs = [
        {Node, segment(n1, 0, 20)},
        {node1, segment(n2, 0, 10)},
        {Node, segment(n3, 20, 40)},
        {node2, segment(n4, 10, 10)},
        {node2, segment(n5, 50, 20)},
        {node3, segment(n6, 20, 20)},
        {Node, segment(n7, 50, 10)},
        {node1, segment(n8, 40, 10)}
    ],
    Asm = append_many(new(70), Segs),
    ?assertMatch(
        {complete,
            [
                {Node, #{path := n1}},
                {node3, #{path := n6}},
                {node1, #{path := n8}},
                {node2, #{path := n5}}
            ],
            #{dominant := _}},
        status(coverage, Asm)
    ).

redundant_coverage_prefer_local_test() ->
    Node = node(),
    Segs = [
        {node1, segment(n1, 0, 20)},
        {Node, segment(n2, 0, 10)},
        {Node, segment(n3, 10, 10)},
        {node2, segment(n4, 20, 20)},
        {Node, segment(n5, 30, 10)},
        {Node, segment(n6, 20, 10)}
    ],
    Asm = append_many(new(40), Segs),
    ?assertMatch(
        {complete,
            [
                {Node, #{path := n2}},
                {Node, #{path := n3}},
                {Node, #{path := n6}},
                {Node, #{path := n5}}
            ],
            #{dominant := Node}},
        status(coverage, Asm)
    ).

missing_coverage_test() ->
    Node = node(),
    Segs = [
        {Node, segment(n1, 0, 10)},
        {node1, segment(n3, 10, 20)},
        {Node, segment(n2, 0, 20)},
        {node2, segment(n4, 50, 50)},
        {Node, segment(n5, 40, 60)}
    ],
    Asm = append_many(new(100), Segs),
    ?assertEqual(
        % {incomplete, {missing, {segment, 30, 40}}}, ???
        {incomplete, {missing, {segment, 20, 40}}},
        status(coverage, Asm)
    ).

missing_end_coverage_test() ->
    Node = node(),
    Segs = [
        {Node, segment(n1, 0, 15)},
        {node1, segment(n3, 10, 10)}
    ],
    Asm = append_many(new(20), Segs),
    ?assertEqual(
        {incomplete, {missing, {segment, 15, 20}}},
        status(coverage, Asm)
    ).

missing_coverage_with_redudancy_test() ->
    Segs = [
        {node(), segment(n1, 0, 10)},
        {node(), segment(n2, 0, 20)},
        {node42, segment(n3, 10, 20)},
        {node43, segment(n4, 10, 50)},
        {node(), segment(n5, 40, 60)}
    ],
    Asm = append_many(new(100), Segs),
    ?assertEqual(
        % {incomplete, {missing, {segment, 50, 60}}}, ???
        {incomplete, {missing, {segment, 20, 40}}},
        status(coverage, Asm)
    ).

append_many(Asm, List) ->
    lists:foldl(
        fun({Node, Frag}, Acc) -> append(Acc, Node, Frag) end,
        Asm,
        List
    ).

filemeta(Path, Name) ->
    #{
        path => Path,
        fragment =>
            {filemeta, #{
                name => Name
            }}
    }.

segment(Path, Offset, Size) ->
    #{
        path => Path,
        fragment =>
            {segment, #{
                offset => Offset,
                size => Size
            }}
    }.

-endif.
