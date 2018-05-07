%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains bp_tree module tests.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_test).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test functions
%%====================================================================

new_should_use_defaults_test() ->
    ?assertMatch({ok, #bp_tree{
        order = 50,
        store_module = bp_tree_map_store
    }}, bp_tree:init()).

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
        }}, bp_tree:init([
            {order, 32},
            {store_module, StoreModule},
            {store_args, StoreArgs}
        ]))
    }.


find_should_return_not_found_error_test() ->
    {ok, Tree} = bp_tree:init([]),
    {{error, not_found}, Tree} = bp_tree:find(<<"someKey">>, Tree).

insert_find_permutation_should_succeed_test() ->
    lists:foreach(fun(Seq) ->
        {ok, Tree} = bp_tree:init([{order, 1}]),
        find(Seq, insert(Seq, Tree))
    end, permute(lists:seq(1, 7))).

insert_find_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:init([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() -> find(Seq, insert(Seq, Tree)) end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

insert_remove_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:init([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() -> remove(Seq, insert(Seq, Tree)) end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

insert_remove_fold_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:init([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() ->
                remove_and_fold(Seq, insert(Seq, Tree))
            end} | Tests2]
        end, Tests, [10, 50, 100, 200, 500])
    end, [], [2, 5, 10, 15, 20, 50])).

fold_should_return_not_found_error_test() ->
    {ok, Tree} = bp_tree:init([]),
    {{error, not_found}, _} = bp_tree:fold(fun(K, _V, Acc) ->
        [K | Acc]
    end, [], Tree).

fold_should_process_keys_in_ascending_order_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:init([{order, Order}]),
            Seq = lists:seq(1, Size),
            RandomSeq = random_shuffle(Seq),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() ->
                Tree2 = insert(RandomSeq, Tree),
                {ok, Keys, _} = fold({offset, 0}, fun(K, _V, Acc) ->
                    [K | Acc]
                end, [], Tree2),
                ?assertEqual(Seq, Keys)
            end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

fold_should_return_keys_from_start_key_test_() ->
    {ok, Tree} = bp_tree:init([{order, 1}]),
    End = 100,
    Seq = lists:seq(1, End),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    lists:foldl(fun(Start, Tests) ->
        Name = io_lib:format("start: ~p", [Start]),
        Name2 = lists:flatten(Name),
        [{Name2, fun() ->
            {ok, Keys, _} = fold({start_key, Start}, fun(K, _V, Acc) ->
                [K | Acc]
            end, [], Tree2),
            ?assertEqual(lists:seq(Start, End), Keys)
        end} | Tests]
    end, [], lists:seq(1, End)).

fold_should_return_keys_from_offset_test_() ->
    {ok, Tree} = bp_tree:init([{order, 1}]),
    End = 100,
    Seq = lists:seq(1, End),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    lists:map(fun(Offset) ->
        Name = io_lib:format("offset: ~p", [Offset]),
        Name2 = lists:flatten(Name),
        {Name2, fun() ->
            {ok, Keys, _} = fold({offset, Offset}, fun(K, _V, Acc) ->
                [K | Acc]
            end, [], Tree2),
            ?assertEqual(lists:seq(Offset + 1, End), Keys)
        end}
    end, lists:seq(0, End)).

prev_node_test() ->
    {ok, Tree} = bp_tree:init([{order, 1}]),
    Start = 1,
    End = 100,
    Seq = lists:seq(Start, End),
    RandomSeq = random_shuffle(Seq),
    Tree2 = insert(RandomSeq, Tree),
    {ok, Keys, _} = fold_and_check_prev_nodes({start_key, Start}, fun(K, _V, Acc) ->
        [K | Acc]
    end, [], Tree2, undefined),
    ?assertEqual(Seq, Keys).

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

fold(Arg, Fun, Acc, Tree) ->
    case bp_tree:fold(Arg, Fun, Acc, Tree) of
        {{ok, {Acc2, undefined}}, Tree2} -> {ok, lists:reverse(Acc2), Tree2};
        {{ok, {Acc2, N}}, Tree2} -> fold({node_id, N}, Fun, Acc2, Tree2);
        {{error, not_found}, Tree2} -> {ok, lists:reverse(Acc), Tree2};
        {{error, Reason}, Tree2} -> {{error, Reason}, Tree2}
    end.

fold_and_check_prev_nodes(Arg, Fun, Acc, Tree0, PrevNode) ->
    {PrevNode2, Tree} = check_prev_node(PrevNode, Tree0, Arg),
    case bp_tree:fold(Arg, Fun, Acc, Tree) of
        {{ok, {Acc2, undefined}}, Tree2} -> {ok, lists:reverse(Acc2), Tree2};
        {{ok, {Acc2, N}}, Tree2} -> fold_and_check_prev_nodes({node_id, N},
            Fun, Acc2, Tree2, PrevNode2);
        {{error, not_found}, Tree2} -> {ok, lists:reverse(Acc), Tree2};
        {{error, Reason}, Tree2} -> {{error, Reason}, Tree2}
    end.

check_prev_node(PrevNode, Tree, {start_key, Key}) ->
    {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
    {[{NodeID, _Leaf} | _], Tree3} = bp_tree_path:find(Key, RootId, Tree2),
    check_prev_node(PrevNode, Tree3, {node_id, NodeID});
check_prev_node(PrevNode, Tree, {node_id, N}) ->
    Tree2 = assert_prev_node(N, PrevNode, Tree),
    {N, Tree2}.

assert_prev_node(CurrentNode, PrevNode, Tree) ->
    {PrevNodeID, _, Tree2} = bp_tree:get_prev_leaf(CurrentNode, Tree),
    ?assertEqual(PrevNode, PrevNodeID),
    Tree2.

fold(Seq, Tree) ->
    {ok, L, Tree2} = fold({offset, 0}, fun(K, _V, A) -> [K | A] end, [], Tree),
    ?assertEqual(lists:sort(Seq), L),
    Tree2.

permute([]) -> [[]];
permute(Seq) -> [[X | Y] || X <- Seq, Y <- permute(Seq -- [X])].

random_shuffle(Seq) ->
    [Y || {_, Y} <- lists:sort([{rand:uniform(), X} || X <- Seq])].