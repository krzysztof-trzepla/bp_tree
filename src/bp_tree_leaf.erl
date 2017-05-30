%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides search functionality for leaf nodes of B+ tree.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_leaf).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([find_leftmost/1, find_key/2]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns leftmost leaf in a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec find_leftmost(bp_tree:tree()) ->
    {{ok, bp_tree_node:id(), bp_tree:tree_node()} | {error, term()},
        bp_tree:tree()}.
find_leftmost(Tree) ->
    case bp_tree_store:get_root_id(Tree) of
        {{ok, RootId}, Tree2} -> find_leftmost(RootId, Tree2);
        {{error, Reason}, Tree2} -> {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns key at given offset starting from leftmost node. Along with the key
%% a node holding it and its position within this node is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_key(non_neg_integer(), bp_tree:tree()) -> {
    {ok, bp_tree:key(), pos_integer(), bp_tree_node:id(), bp_tree:tree_node()} |
    {error, term()
    }, bp_tree:tree()}.
find_key(Offset, Tree) ->
    case find_leftmost(Tree) of
        {{ok, NodeId, Node}, Tree2} ->
            find_key(Offset, NodeId, Node, Tree2);
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns leftmost leaf in a B+ tree rooted in a node.
%% @end
%%--------------------------------------------------------------------
-spec find_leftmost(bp_tree_node:id(), bp_tree:tree()) ->
    {{ok, bp_tree_node:id(), bp_tree:tree_node()} | {error, term()},
        bp_tree:tree()}.
find_leftmost(NodeId, Tree) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    case bp_tree_node:leftmost_child(Node) of
        {ok, NodeId2} -> find_leftmost(NodeId2, Tree2);
        {error, not_found} -> {{ok, NodeId, Node}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key at given offset starting from a node. Along with the key
%% a node holding it and its position within this node is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_key(non_neg_integer(), bp_tree_node:id(), bp_tree:tree_node(),
    bp_tree:tree()) -> {{ok, bp_tree:key(), pos_integer(), bp_tree_node:id(),
    bp_tree:tree_node()} | {error, term()}, bp_tree:tree()}.
find_key(Offset, NodeId, Node, Tree) ->
    Size = bp_tree_node:size(Node),
    case Size =< Offset of
        true ->
            case bp_tree_node:right_sibling(Node) of
                {ok, NodeId2} ->
                    {{ok, Node2}, Tree2} = bp_tree_store:get_node(NodeId2, Tree),
                    find_key(Offset - Size, NodeId2, Node2, Tree2);
                {error, Reason} ->
                    {{error, Reason}, Tree}
            end;
        false ->
            {ok, Key} = bp_tree_node:key(Offset + 1, Node),
            {{ok, Key, Offset + 1, NodeId, Node}, Tree}
    end.