%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(prop_emqx_ft_assembly).

-include_lib("proper/include/proper.hrl").

-import(emqx_proper_types, [scaled/2, logscaled/2, fixedmap/1, typegen/0, generate/2]).

-define(COVERAGE_TIMEOUT, 10000).

prop_coverage() ->
    ?FORALL(
        #{filesize := Filesize, segsizes := Segsizes, typegen := TypeGen},
        noshrink(
            fixedmap(#{
                filesize => filesize_t(),
                segsizes => segsizes_t(),
                typegen => typegen()
            })
        ),
        ?TIMEOUT(
            ?COVERAGE_TIMEOUT,
            begin
                Segments = generate(segments_t(Filesize, Segsizes), TypeGen),
                ASM1 = append_segments(mk_assembly(Filesize), Segments),
                {Time, ASM2} = timer:tc(emqx_ft_assembly, update, [ASM1]),
                measure(
                    #{"Segments" => length(Segments), "Time" => Time},
                    case emqx_ft_assembly:status(ASM2) of
                        complete ->
                            Coverage = emqx_ft_assembly:coverage(ASM2),
                            measure(
                                #{"CoverageLength" => length(Coverage)},
                                is_coverage_complete(Coverage)
                            );
                        {incomplete, {missing, {segment, _, _}}} ->
                            measure("CoverageLength", 0, true)
                    end
                )
            end
        )
    ).

prop_coverage_likely_incomplete() ->
    ?FORALL(
        #{filesize := Filesize, segsizes := Segsizes, hole := HoleIn, typegen := TypeGen},
        noshrink(
            fixedmap(#{
                filesize => filesize_t(),
                segsizes => segsizes_t(),
                hole => filesize_t(),
                typegen => typegen()
            })
        ),
        ?TIMEOUT(?COVERAGE_TIMEOUT, begin
            Hole = HoleIn rem max(Filesize, 1),
            Segments = generate(segments_t(Filesize, Segsizes, Hole), TypeGen),
            ASM1 = append_segments(mk_assembly(Filesize), Segments),
            {Time, ASM2} = timer:tc(emqx_ft_assembly, update, [ASM1]),
            measure(
                #{"Segments" => length(Segments), "Time" => Time},
                case emqx_ft_assembly:status(ASM2) of
                    complete ->
                        % NOTE: this is still possible due to the nature of `SUCHTHATMAYBE`
                        IsComplete = emqx_ft_assembly:coverage(ASM2),
                        collect(complete, is_coverage_complete(IsComplete));
                    {incomplete, {missing, {segment, _, _}}} ->
                        collect(incomplete, true)
                end
            )
        end)
    ).

prop_coverage_complete() ->
    ?FORALL(
        #{filesize := Filesize, segsizes := Segsizes, node := RemoteNode, typegen := TypeGen},
        noshrink(
            fixedmap(#{
                filesize => filesize_t(),
                segsizes => ?SUCHTHAT([BaseSegsize | _], segsizes_t(), BaseSegsize > 0),
                node => remote_node_t(),
                typegen => typegen()
            })
        ),
        ?TIMEOUT(
            ?COVERAGE_TIMEOUT,
            begin
                % Ensure that we have complete coverage
                Segments = generate(segments_t(Filesize, Segsizes), TypeGen),
                ASM1 = mk_assembly(Filesize),
                ASM2 = append_coverage(ASM1, RemoteNode, Filesize, Segsizes),
                ASM3 = append_segments(ASM2, Segments),
                {Time, ASM4} = timer:tc(emqx_ft_assembly, update, [ASM3]),
                measure(
                    #{"CoverageMax" => nsegs(Filesize, Segsizes), "Time" => Time},
                    case emqx_ft_assembly:status(ASM4) of
                        complete ->
                            Coverage = emqx_ft_assembly:coverage(ASM4),
                            measure(
                                #{"Coverage" => length(Coverage)},
                                is_coverage_complete(Coverage)
                            );
                        {incomplete, _} ->
                            false
                    end
                )
            end
        )
    ).

measure(NamedSamples, Test) ->
    maps:fold(fun(Name, Sample, Acc) -> measure(Name, Sample, Acc) end, Test, NamedSamples).

is_coverage_complete([]) ->
    true;
is_coverage_complete(Coverage = [_ | Tail]) ->
    is_coverage_complete(Coverage, Tail).

is_coverage_complete([_], []) ->
    true;
is_coverage_complete(
    [{_Node1, #{fragment := {segment, #{offset := O1, size := S1}}}} | Rest],
    [{_Node2, #{fragment := {segment, #{offset := O2}}}} | Tail]
) ->
    (O1 + S1 == O2) andalso is_coverage_complete(Rest, Tail).

mk_assembly(Filesize) ->
    emqx_ft_assembly:append(emqx_ft_assembly:new(Filesize), node(), mk_filemeta(Filesize)).

append_segments(ASMIn, Fragments) ->
    lists:foldl(
        fun({Node, {Offset, Size}}, ASM) ->
            emqx_ft_assembly:append(ASM, Node, mk_segment(Offset, Size))
        end,
        ASMIn,
        Fragments
    ).

append_coverage(ASM, Node, Filesize, Segsizes = [BaseSegsize | _]) ->
    append_coverage(ASM, Node, Filesize, BaseSegsize, 0, nsegs(Filesize, Segsizes)).

append_coverage(ASM, Node, Filesize, Segsize, I, NSegs) when I < NSegs ->
    Offset = I * Segsize,
    Size = min(Segsize, Filesize - Offset),
    ASMNext = emqx_ft_assembly:append(ASM, Node, mk_segment(Offset, Size)),
    append_coverage(ASMNext, Node, Filesize, Segsize, I + 1, NSegs);
append_coverage(ASM, _Node, _Filesize, _Segsize, _, _NSegs) ->
    ASM.

mk_filemeta(Filesize) ->
    #{
        path => "MANIFEST.json",
        fragment => {filemeta, #{name => ?MODULE_STRING, size => Filesize}}
    }.

mk_segment(Offset, Size) ->
    #{
        path => "SEG" ++ integer_to_list(Offset) ++ integer_to_list(Size),
        fragment => {segment, #{offset => Offset, size => Size}}
    }.

nsegs(Filesize, [BaseSegsize | _]) ->
    Filesize div max(1, BaseSegsize) + 1.

segments_t(Filesize, Segsizes) ->
    logscaled(nsegs(Filesize, Segsizes), list({node_t(), segment_t(Filesize, Segsizes)})).

segments_t(Filesize, Segsizes, Hole) ->
    logscaled(nsegs(Filesize, Segsizes), list({node_t(), segment_t(Filesize, Segsizes, Hole)})).

segment_t(Filesize, Segsizes, Hole) ->
    ?SUCHTHATMAYBE(
        {Offset, Size},
        segment_t(Filesize, Segsizes),
        Hole =< Offset orelse Hole > (Offset + Size)
    ).

segment_t(Filesize, Segsizes) ->
    ?LET(
        Segsize,
        oneof(Segsizes),
        ?LET(
            Index,
            range(0, Filesize div max(1, Segsize)),
            {Index * Segsize, min(Segsize, Filesize - (Index * Segsize))}
        )
    ).

filesize_t() ->
    scaled(2000, non_neg_integer()).

segsizes_t() ->
    ?LET(
        BaseSize,
        segsize_t(),
        oneof([
            [BaseSize, BaseSize * 2],
            [BaseSize, BaseSize * 2, BaseSize * 3],
            [BaseSize, BaseSize * 2, BaseSize * 5]
        ])
    ).

segsize_t() ->
    scaled(50, non_neg_integer()).

remote_node_t() ->
    oneof([
        'emqx42@emqx.local',
        'emqx43@emqx.local',
        'emqx44@emqx.local'
    ]).

node_t() ->
    oneof([
        node(),
        'emqx42@emqx.local',
        'emqx43@emqx.local',
        'emqx44@emqx.local'
    ]).
