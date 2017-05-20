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
    lists:foreach(fun(Permutation) ->
        {ok, Tree} = bp_tree:new([{order, 1}]),
        insert_find(Tree, Permutation)
    end, permute(lists:seq(1, 7))).

insert_find_random_seq_should_succeed_test_() ->
    lists:reverse(lists:foldl(fun(Order, Tests) ->
        lists:foldl(fun(Size, Tests2) ->
            {ok, Tree} = bp_tree:new([{order, Order}]),
            Seq = random_shuffle(lists:seq(1, Size)),
            Name = io_lib:format("order: ~p, size: ~p", [Order, Size]),
            Name2 = lists:flatten(Name),
            [{Name2, fun() -> insert_find(Tree, Seq) end} | Tests2]
        end, Tests, [10, 50, 100, 500, 1000, 5000, 10000])
    end, [], [1, 2, 5, 10, 50, 100])).

%%====================================================================
%% Internal functions
%%====================================================================

insert_find(Tree, Seq) ->
    Tree4 = lists:foldl(fun(X, Tree2) ->
        {ok, Tree3} = bp_tree:insert(X, X, Tree2),
        Tree3
    end, Tree, Seq),
    lists:foreach(fun(X) ->
        ?assertMatch({{ok, X}, _}, bp_tree:find(X, Tree4))
    end, Seq).

permute([]) -> [[]];
permute(Seq) -> [[X | Y] || X <- Seq, Y <- permute(Seq -- [X])].

random_shuffle(Seq) ->
    [Y || {_, Y} <- lists:sort([{rand:uniform(), X} || X <- Seq])].
