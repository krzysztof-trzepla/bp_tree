%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains bp_tree_leaf module tests.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_leaf_test).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test functions
%%====================================================================

find_next_should_return_leaf_containing_next_key_test() ->
    {ok, Tree} = bp_tree:init([{order, 2}]),
    Size = 1000,
    Tree2 = insert(lists:seq(1, Size), Tree),
    lists:foldl(fun(X, Tree3) ->
        {{ok, Pos, Node}, Tree4} = bp_tree_leaf:find_next(X, Tree3),
        ?assertEqual({ok, X + 1}, bp_tree_node:key(Pos, Node)),
        Tree4
    end, Tree2, lists:seq(1, Size - 1)).

%%====================================================================
%% Internal functions
%%====================================================================

insert([], Tree) ->
    Tree;
insert([X | Seq], Tree) ->
    {ok, Tree2} = bp_tree:insert(X, X, Tree),
    insert(Seq, Tree2).