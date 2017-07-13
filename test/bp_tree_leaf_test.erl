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
-author("krzysztof").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test functions
%%====================================================================

find_next_should_return_leaf_containing_next_key_test() ->
    {ok, Tree} = bp_tree:init([{order, 2}]),
    Tree2 = insert(lists:seq(1, 100), Tree),
    {{ok, RootId}, Tree3} = bp_tree_store:get_root_id(Tree2),
    lists:foldl(fun(X, Tree4) ->
        {{ok, Pos, Node}, Tree5} = bp_tree_leaf:find_next(X, RootId, Tree4),
        ?assertEqual({ok, X + 1}, bp_tree_node:key(Pos, Node)),
        Tree5
    end, Tree3, lists:seq(1, 99)).

%%====================================================================
%% Internal functions
%%====================================================================

insert([], Tree) ->
    Tree;
insert([X | Seq], Tree) ->
    {ok, Tree2} = bp_tree:insert(X, X, Tree),
    insert(Seq, Tree2).