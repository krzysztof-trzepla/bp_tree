%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains bp_tree_array module tests.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_array_test).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(K, ?K(1)).
-define(K(N), <<"key-", (integer_to_binary(N))/binary>>).
-define(V, <<"value">>).

insert_should_return_out_of_space_error_test() ->
    A = bp_tree_array:new(0),
    ?assertEqual({error, out_of_space}, bp_tree_array:insert({left, ?K}, ?V, A)),
    ?assertEqual({error, out_of_space}, bp_tree_array:insert({key, ?K}, ?V, A)),
    ?assertEqual({error, out_of_space}, bp_tree_array:insert({right, ?K}, ?V, A)),
    ?assertEqual({error, out_of_space}, bp_tree_array:insert({both, ?K}, {?V, ?V}, A)).

insert_should_succeed_test() ->
    A = bp_tree_array:new(10),
    ?assertMatch({ok, _}, bp_tree_array:insert({left, ?K}, ?V, A)),
    ?assertMatch({ok, _}, bp_tree_array:insert({key, ?K}, ?V, A)),
    ?assertMatch({ok, _}, bp_tree_array:insert({right, ?K}, ?V, A)),
    ?assertMatch({ok, _}, bp_tree_array:insert({both, ?K}, {?V, ?V}, A)).

insert_should_return_already_exists_error_test() ->
    A = bp_tree_array:new(10),
    {ok, A2} = bp_tree_array:insert({left, ?K}, ?V, A),
    ?assertEqual({error, already_exists}, bp_tree_array:insert({left, ?K}, ?V, A2)).

insert_should_maintain_order_test_() ->
    {foreachx,
        fun(Keys) ->
            A = lists:foldl(fun(Key, A2) ->
                {ok, A3} = bp_tree_array:insert({both, Key}, {?V, ?V}, A2),
                A3
            end, bp_tree_array:new(5), Keys),
            Keys2 = lists:reverse(lists:sort(Keys)),
            List = lists:foldl(fun(Key, Acc) ->
                [?V, Key | Acc]
            end, [?V], Keys2),
            {A, List}
        end,
        fun(_, _) -> ok end,
        [
            {[?K(1), ?K(2), ?K(3), ?K(4), ?K(5)], fun(_, {A, List}) ->
                {"ordered", ?_assertEqual(List, bp_tree_array:to_list(A))}
            end},
            {[?K(5), ?K(4), ?K(3), ?K(2), ?K(1)], fun(_, {A, List}) ->
                {"reversed", ?_assertEqual(List, bp_tree_array:to_list(A))}
            end},
            {[?K(3), ?K(1), ?K(5), ?K(4), ?K(2)], fun(_, {A, List}) ->
                {"random", ?_assertEqual(List, bp_tree_array:to_list(A))}
            end}
        ]
    }.

remove_should_return_empty_error_test() ->
    A = bp_tree_array:new(3),
    ?assertEqual({error, not_found}, bp_tree_array:remove({left, ?K}, A)).

remove_should_return_not_found_error_test() ->
    A = bp_tree_array:from_list([?V, ?K(1), ?V, ?K(3), ?V]),
    ?assertEqual({error, not_found}, bp_tree_array:remove({left, ?K(2)}, A)).

remove_should_succeed_test() ->
    A = bp_tree_array:from_list([?V, ?K(1), ?V, ?K(3), ?V, ?K(5), ?V]),
    A2 = bp_tree_array:from_list([?V, ?K(1), ?V, ?K(5), ?V, ?NIL, ?NIL]),
    A3 = bp_tree_array:from_list([?V, ?K(5), ?V, ?NIL, ?NIL, ?NIL, ?NIL]),
    A4 = bp_tree_array:from_list([?V, ?NIL, ?NIL, ?NIL, ?NIL, ?NIL, ?NIL]),
    ?assertEqual({ok, A2}, bp_tree_array:remove({left, ?K(3)}, A)),
    ?assertEqual({ok, A3}, bp_tree_array:remove({left, ?K(1)}, A2)),
    ?assertEqual({ok, A4}, bp_tree_array:remove({left, ?K(5)}, A3)).

accessor_should_succeed_test_() ->
    {setup,
        fun() -> bp_tree_array:from_list([1, ?K(2), 3, ?K(4), 5]) end,
        fun(_) -> ok end,
        {with, [
            fun(A) ->
                ?assertEqual({ok, 1}, bp_tree_array:get({left, 1}, A)),
                ?assertEqual({ok, ?K(2)}, bp_tree_array:get({key, 1}, A)),
                ?assertEqual({ok, 3}, bp_tree_array:get({right, 1}, A))
            end,
            fun(A) ->
                ?assertEqual({ok, 3}, bp_tree_array:get({left, 2}, A)),
                ?assertEqual({ok, ?K(4)}, bp_tree_array:get({key, 2}, A)),
                ?assertEqual({ok, 5}, bp_tree_array:get({right, 2}, A))
            end
        ]}
    }.

accessor_should_return_out_of_range_error_test_() ->
    {setup,
        fun() -> bp_tree_array:from_list([?V, ?K(2), ?V, ?K(4), ?V]) end,
        fun(_) -> ok end,
        {with, [
            fun(A) ->
                ?assertEqual({error, out_of_range}, bp_tree_array:get({left, 0}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_array:get({key, 0}, A)),
                ?assertEqual({ok, ?V}, bp_tree_array:get({right, 0}, A))
            end,
            fun(A) ->
                ?assertEqual({error, out_of_range}, bp_tree_array:get({left, 3}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_array:get({key, 3}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_array:get({right, 3}, A))
            end,
            fun(A) ->
                ?assertEqual({error, out_of_range}, bp_tree_array:get({left, 5}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_array:get({key, 5}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_array:get({right, 5}, A))
            end
        ]}
    }.

find_should_succeed_test_() ->
    {setup,
        fun() ->
            bp_tree_array:from_list([?V, ?K(1), ?V, ?K(3), ?V, ?K(5), ?V])
        end,
        fun(_) -> ok end,
        {with, [
            fun(M) -> ?assertEqual({ok, 1}, bp_tree_array:find(?K(1), M)) end,
            fun(M) -> ?assertEqual({ok, 2}, bp_tree_array:find(?K(3), M)) end,
            fun(M) -> ?assertEqual({ok, 3}, bp_tree_array:find(?K(5), M)) end
        ]}
    }.

find_should_return_not_found_error_test_() ->
    {setup,
        fun() ->
            A = bp_tree_array:from_list([?V, ?K(1), ?V, ?K(3), ?V, ?K(5), ?V]),
            {A, {error, not_found}}
        end,
        fun(_) -> ok end,
        {with, [
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_array:find(?K(0), A)) end,
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_array:find(?K(2), A)) end,
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_array:find(?K(4), A)) end,
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_array:find(?K(6), A))
            end
        ]}
    }.

lower_bound_should_succeed_test_() ->
    {setup,
        fun() ->
            bp_tree_array:from_list([?V, ?K(1), ?V, ?K(3), ?V, ?K(5), ?V])
        end,
        fun(_) -> ok end,
        {with, [
            fun(A) -> ?assertEqual(1, bp_tree_array:lower_bound(?K(0), A)) end,
            fun(A) -> ?assertEqual(1, bp_tree_array:lower_bound(?K(1), A)) end,
            fun(A) -> ?assertEqual(2, bp_tree_array:lower_bound(?K(2), A)) end,
            fun(A) -> ?assertEqual(2, bp_tree_array:lower_bound(?K(3), A)) end,
            fun(A) -> ?assertEqual(3, bp_tree_array:lower_bound(?K(4), A)) end,
            fun(A) -> ?assertEqual(3, bp_tree_array:lower_bound(?K(5), A)) end,
            fun(A) -> ?assertEqual(4, bp_tree_array:lower_bound(?K(6), A)) end
        ]}
    }.

size_should_succeed_test() ->
    A = bp_tree_array:new(3),
    ?assertEqual(0, bp_tree_array:size(A)),
    {ok, A2} = bp_tree_array:insert({left, ?K(1)}, ?V, A),
    ?assertEqual(1, bp_tree_array:size(A2)),
    {ok, A3} = bp_tree_array:insert({left, ?K(2)}, ?V, A2),
    ?assertEqual(2, bp_tree_array:size(A3)).

to_list_should_succeed_test() ->
    A = bp_tree_array:new(2),
    {ok, A2} = bp_tree_array:insert({left, ?K(1)}, ?V, A),
    {ok, A3} = bp_tree_array:insert({left, ?K(2)}, ?V, A2),
    ?assertEqual([?V, ?K(1), ?V, ?K(2), ?NIL], bp_tree_array:to_list(A3)).

from_list_should_succeed_test() ->
    A = bp_tree_array:new(2),
    {ok, A2} = bp_tree_array:insert({left, ?K(1)}, ?V, A),
    {ok, A3} = bp_tree_array:insert({left, ?K(2)}, ?V, A2),
    ?assertEqual(A3, bp_tree_array:from_list([?V, ?K(1), ?V, ?K(2), ?NIL])).
