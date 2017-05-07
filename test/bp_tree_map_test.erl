%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_map_test).
-author("krzysztof").

-include_lib("eunit/include/eunit.hrl").

insert_should_return_out_of_space_error_test() ->
    Map = bp_tree_map:new(0),
    ?assertEqual({error, out_of_space}, bp_tree_map:insert(k, v, Map)).

insert_should_succeed_test() ->
    Map = bp_tree_map:new(10),
    ?assertMatch({ok, _}, bp_tree_map:insert(k, v, Map)).

insert_should_return_already_exists_error_test() ->
    Map = bp_tree_map:new(10),
    {ok, Map2} = bp_tree_map:insert(k, v, Map),
    ?assertEqual({error, already_exists}, bp_tree_map:insert(k, v, Map2)).

insert_should_maintain_order_test_() ->
    {foreachx,
        fun(Keys) ->
            Pairs = lists:zip(Keys, lists:duplicate(erlang:length(Keys), v)),
            Map = lists:foldl(fun(Key, Map2) ->
                {ok, Map3} = bp_tree_map:insert(Key, v, Map2),
                Map3
            end, bp_tree_map:new(5), Keys),
            {Map, lists:sort(Pairs)}
        end,
        fun(_, _) -> ok end,
        [
            {[k1, k2, k3, k4, k5], fun(_, {Map, Pairs}) -> {"ordered",
                ?_assertEqual(Pairs, bp_tree_map:to_list(Map))
            } end},
            {[k5, k4, k3, k2, k1], fun(_, {Map, Pairs}) -> {"reversed",
                ?_assertEqual(Pairs, bp_tree_map:to_list(Map))
            } end},
            {[k3, k1, k5, k4, k2], fun(_, {Map, Pairs}) -> {"random",
                ?_assertEqual(Pairs, bp_tree_map:to_list(Map))
            } end}
        ]
    }.

at_should_succeed_test_() ->
    {setup,
        fun() ->
            Map = bp_tree_map:new(3),
            {ok, Map2} = bp_tree_map:insert(k2, v2, Map),
            {ok, Map3} = bp_tree_map:insert(k4, v4, Map2),
            Map3
        end,
        fun(_) -> ok end,
        {with, [
            fun(Map) ->
                ?assertEqual({ok, {k2, v2}}, bp_tree_map:at(1, Map))
            end,
            fun(Map) ->
                ?assertEqual({ok, {k4, v4}}, bp_tree_map:at(2, Map))
            end
        ]}
    }.

at_should_return_out_of_range_error_test_() ->
    {setup,
        fun() ->
            Map = bp_tree_map:new(3),
            {ok, Map2} = bp_tree_map:insert(k2, v4, Map),
            {ok, Map3} = bp_tree_map:insert(k4, v4, Map2),
            Map3
        end,
        fun(_) -> ok end,
        {with, [
            fun(Map) ->
                ?assertEqual({error, out_of_range}, bp_tree_map:at(0, Map))
            end,
            fun(Map) ->
                ?assertEqual({error, out_of_range}, bp_tree_map:at(3, Map))
            end
        ]}
    }.

find_should_succeed_test_() ->
    {setup,
        fun() ->
            Map = bp_tree_map:new(3),
            {ok, Map2} = bp_tree_map:insert(k1, v1, Map),
            {ok, Map3} = bp_tree_map:insert(k3, v3, Map2),
            {ok, Map4} = bp_tree_map:insert(k5, v5, Map3),
            Map4
        end,
        fun(_) -> ok end,
        {with, [
            fun(Map) -> ?assertEqual({ok, v1}, bp_tree_map:find(k1, Map)) end,
            fun(Map) -> ?assertEqual({ok, v3}, bp_tree_map:find(k3, Map)) end,
            fun(Map) -> ?assertEqual({ok, v5}, bp_tree_map:find(k5, Map)) end
        ]}
    }.

find_should_return_not_found_error_test_() ->
    {setup,
        fun() ->
            Map = bp_tree_map:new(3),
            {ok, Map2} = bp_tree_map:insert(k1, v1, Map),
            {ok, Map3} = bp_tree_map:insert(k3, v3, Map2),
            {ok, Map4} = bp_tree_map:insert(k5, v5, Map3),
            {Map4, {error, not_found}}
        end,
        fun(_) -> ok end,
        {with, [
            fun({Map, Err}) -> ?assertEqual(Err, bp_tree_map:find(k0, Map)) end,
            fun({Map, Err}) -> ?assertEqual(Err, bp_tree_map:find(k2, Map)) end,
            fun({Map, Err}) -> ?assertEqual(Err, bp_tree_map:find(k4, Map)) end,
            fun({Map, Err}) -> ?assertEqual(Err, bp_tree_map:find(k6, Map)) end
        ]}
    }.

lower_bound_should_succeed_test_() ->
    {setup,
        fun() ->
            Map = bp_tree_map:new(3),
            {ok, Map2} = bp_tree_map:insert(k1, v1, Map),
            {ok, Map3} = bp_tree_map:insert(k3, v3, Map2),
            {ok, Map4} = bp_tree_map:insert(k5, v5, Map3),
            Map4
        end,
        fun(_) -> ok end,
        {with, [
            fun(Map) -> ?assertEqual(1, bp_tree_map:lower_bound(k0, Map)) end,
            fun(Map) -> ?assertEqual(1, bp_tree_map:lower_bound(k1, Map)) end,
            fun(Map) -> ?assertEqual(2, bp_tree_map:lower_bound(k2, Map)) end,
            fun(Map) -> ?assertEqual(2, bp_tree_map:lower_bound(k3, Map)) end,
            fun(Map) -> ?assertEqual(3, bp_tree_map:lower_bound(k4, Map)) end,
            fun(Map) -> ?assertEqual(3, bp_tree_map:lower_bound(k5, Map)) end,
            fun(Map) -> ?assertEqual(4, bp_tree_map:lower_bound(k6, Map)) end
        ]}
    }.

size_should_succeed_test() ->
    Map = bp_tree_map:new(3),
    ?assertEqual(0, bp_tree_map:size(Map)),
    {ok, Map2} = bp_tree_map:insert(k1, v1, Map),
    ?assertEqual(1, bp_tree_map:size(Map2)),
    {ok, Map3} = bp_tree_map:insert(k2, v2, Map2),
    ?assertEqual(2, bp_tree_map:size(Map3)).

to_list_should_succeed_test() ->
    Map = bp_tree_map:new(3),
    {ok, Map2} = bp_tree_map:insert(k1, v1, Map),
    {ok, Map3} = bp_tree_map:insert(k2, v2, Map2),
    ?assertEqual([{k1, v1}, {k2, v2}], bp_tree_map:to_list(Map3)).

from_list_should_succeed_test() ->
    Map = bp_tree_map:new(3),
    {ok, Map2} = bp_tree_map:insert(k1, v1, Map),
    {ok, Map3} = bp_tree_map:insert(k2, v2, Map2),
    ?assertEqual(Map3, bp_tree_map:from_list([{k1, v1}, {k2, v2}], 3)).



