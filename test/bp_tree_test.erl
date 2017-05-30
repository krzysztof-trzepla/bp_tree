%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_test).
-author("krzysztof").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test functions
%%====================================================================

new_should_use_defaults_test() ->
    ?assertMatch({ok, #bp_tree{
        order = 50,
        store_module = bp_tree_map_store
    }}, bp_tree:new()).

new_should_use_custom_options_test_() ->
    StoreModule = some_module,
    StoreArgs = [arg1, arg2, arg3],
    {setup,
        fun() ->
            meck:new(StoreModule, [non_strict]),
            meck:expect(StoreModule, init, fun(Args) when Args =:= StoreArgs ->
                {ok, state}
            end)
        end,
        fun(_) ->
            ?assert(meck:validate(StoreModule)),
            meck:unload(StoreModule)
        end,
        ?_assertEqual({ok, #bp_tree{
            order = 32,
            store_module = StoreModule,
            store_state = state
        }}, bp_tree:new([
            {order, 32},
            {store_module, StoreModule},
            {store_args, StoreArgs}
        ]))
    }.

insert_find_permutation_should_succeed_test() ->
    lists:foreach(fun(Seq) ->
        {ok, Tree} = bp_tree:new([{order, 1}]),
        find(Seq, insert(Seq, Tree))
    end, permute(lists:seq(1, 7))).

insert_find_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:new([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() -> find(Seq, insert(Seq, Tree)) end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

insert_remove_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:new([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() -> remove(Seq, insert(Seq, Tree)) end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

insert_remove_fold_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:new([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() ->
                remove_and_fold(Seq, insert(Seq, Tree))
            end} | Tests2]
        end, Tests, [10, 50, 100, 500])
    end, [], [1, 2, 5, 10, 50])).

fold_should_return_empty_for_empty_tree_test() ->
    {ok, Tree} = bp_tree:new([]),
    {ok, [], _} = bp_tree:fold(fun(K, _V, Acc) ->
        [K | Acc]
    end, [], Tree, []).

fold_should_process_keys_in_ascending_order_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:new([{order, Order}]),
            Seq = lists:seq(1, Size),
            RandomSeq = random_shuffle(Seq),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() ->
                Tree2 = insert(RandomSeq, Tree),
                {ok, Keys, _} = bp_tree:fold(fun(K, _V, Acc) ->
                    [K | Acc]
                end, [], Tree2, []),
                ?assertEqual(Seq, lists:reverse(Keys))
            end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

fold_should_return_keys_from_range_test_() ->
    {ok, Tree} = bp_tree:new([{order, 1}]),
    Seq = lists:seq(1, 10),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    lists:foldl(fun(Start, Tests) ->
        lists:foldl(fun(End, Tests2) ->
            Name = io_lib:format("start: ~p, end: ~p", [Start, End]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() ->
                {ok, Keys, _} = bp_tree:fold(fun(K, _V, Acc) ->
                    [K | Acc]
                end, [], Tree2, [{start_key, Start}, {end_key, End}]),
                ?assertEqual(lists:seq(Start, End), lists:reverse(Keys))
            end} | Tests2]
        end, Tests, lists:seq(Start, 10))
    end, [], lists:seq(1, 10)).

fold_should_return_keys_from_offset_test_() ->
    {ok, Tree} = bp_tree:new([{order, 1}]),
    Seq = lists:seq(1, 100),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    lists:map(fun(Offset) ->
        Name = io_lib:format("offset: ~p", [Offset]),
        Name2 = lists:flatten(Name),
        {Name2, fun() ->
            {ok, Keys, _} = bp_tree:fold(fun(K, _V, Acc) ->
                [K | Acc]
            end, [], Tree2, [{offset, Offset}]),
            ?assertEqual(lists:seq(Offset + 1, 100), lists:reverse(Keys))
        end}
    end, lists:seq(0, 100)).

fold_should_return_up_to_total_size_keys_test_() ->
    {ok, Tree} = bp_tree:new([{order, 2}]),
    Seq = lists:seq(1, 100),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    lists:map(fun(Size) ->
        Name = io_lib:format("total size: ~p", [Size]),
        Name2 = lists:flatten(Name),
        {Name2, fun() ->
            {ok, Keys, _} = bp_tree:fold(fun(K, _V, Acc) ->
                [K | Acc]
            end, [], Tree2, [{total_size, Size}]),
            ?assertEqual(lists:seq(1, Size), lists:reverse(Keys))
        end}
    end, lists:seq(0, 100)).

fold_should_return_keys_in_batch_test() ->
    {ok, Tree} = bp_tree:new([{order, 2}]),
    Seq = lists:seq(1, 100),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    {continue, Token, Keys, Tree3} = bp_tree:fold(fun(K, _V, Acc) ->
        [K | Acc]
    end, [], Tree2, [{batch_size, 50}]),
    ?assertEqual(lists:seq(1, 50), lists:reverse(Keys)),
    {continue, Token2, Keys2, Tree4} = bp_tree:fold(Token, [], Tree3),
    ?assertEqual(lists:seq(51, 100), lists:reverse(Keys2)),
    {ok, [], _} = bp_tree:fold(Token2, [], Tree4).

%%====================================================================
%% Internal functions
%%====================================================================

insert([], Tree) ->
    Tree;
insert([X | Seq], Tree) ->
    {ok, Tree2} = bp_tree:insert(X, X, Tree),
    insert(Seq, Tree2).

find([], Tree) ->
    Tree;
find([X | Seq], Tree) ->
    {{ok, X}, Tree2} = bp_tree:find(X, Tree),
    find(Seq, Tree2).

remove([], Tree) ->
    Tree;
remove([X | Seq], Tree) ->
    {ok, Tree2} = bp_tree:remove(X, Tree),
    remove(Seq, Tree2).

remove_and_fold([], Tree) ->
    fold([], Tree);
remove_and_fold([X | Seq], Tree) ->
    {ok, Tree2} = bp_tree:remove(X, Tree),
    Tree3 = fold(Seq, Tree2),
    remove_and_fold(Seq, Tree3).

fold(Seq, Tree) ->
    {ok, L, Tree2} = bp_tree:fold(fun(K, _V, A) -> [K | A] end, [], Tree, []),
    ?assertEqual(lists:sort(Seq), lists:reverse(L)),
    Tree2.

%%permute(Seq) -> [Seq];
permute([]) -> [[]];
permute(Seq) -> [[X | Y] || X <- Seq, Y <- permute(Seq -- [X])].

random_shuffle(Seq) ->
    [Y || {_, Y} <- lists:sort([{rand:uniform(), X} || X <- Seq])].