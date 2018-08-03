%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an API for a B+ tree.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([init/0, init/1, terminate/1]).
-export([find/2, insert/3, remove/2, remove/3, fold/3, fold/4]).
-export([get_prev_leaf/2]).

-type key() :: term().
-type value() :: term().
-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type order() :: pos_integer().
-type init_opt() :: {order, order()} |
                    {store_module, module()} |
                    {store_args, bp_tree_store:args()}.
-type remove_pred() :: fun((value()) -> boolean()).
-type fold_init() :: {start_key, key()} |
                     {prev_key, key()} |
                     {node_of_key, key()} |
                     {node_prev_to_key, key()} |
                     {offset, non_neg_integer()}.
-type fold_acc() :: any().
-type fold_fun() :: fun((key(), value(), fold_acc()) -> fold_acc()).
-type fold_next_node_id() :: bp_tree_node:id() | undefined.
-type fold_start_spec() :: {pos, pos_integer()} | {key, key()} | all.
-type error() :: {error, term()}.
-type error_stacktrace() :: {error, {term(), [erlang:stack_item()]}}.

-export_type([key/0, value/0, tree/0, tree_node/0, order/0]).
-export_type([remove_pred/0]).
-export_type([fold_init/0, fold_acc/0, fold_fun/0, fold_start_spec/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @equiv init([])
%% @end
%%--------------------------------------------------------------------
-spec init() -> {ok, tree()} | error().
init() ->
    init([]).

%%--------------------------------------------------------------------
%% @doc
%% Creates B+ tree handle.
%% @end
%%--------------------------------------------------------------------
-spec init([init_opt()]) -> {ok, tree()} | error().
init(Opts) ->
    Args = proplists:get_value(store_args, Opts, []),
    Tree = #bp_tree{
        order = proplists:get_value(order, Opts, 50),
        store_module = proplists:get_value(store_module, Opts, bp_tree_map_store)
    },
    bp_tree_store:init(Args, Tree).

%%--------------------------------------------------------------------
%% @doc
%% Cleanups B+ tree handle.
%% @end
%%--------------------------------------------------------------------
-spec terminate(tree()) -> any().
terminate(Tree = #bp_tree{}) ->
    bp_tree_store:terminate(Tree).

%%--------------------------------------------------------------------
%% @doc
%% Returns a value associated with a key from a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec find(key(), tree()) ->
    {{ok, value()} | error() | error_stacktrace(), tree()}.
find(Key, Tree = #bp_tree{}) ->
    try
        {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
        {[{_, Leaf} | _], Tree3} = bp_tree_path:find(Key, RootId, Tree2),
        {bp_tree_node:find(Key, Leaf), Tree3}
    catch
        _:Error -> handle_exception(Error, erlang:get_stacktrace(), Tree)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Inserts a key-value pair into a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), tree()) ->
    {ok | error() | error_stacktrace(), tree()}.
insert(Key, Value, Tree) ->
    try
        case bp_tree_store:get_root_id(Tree) of
            {{ok, RootId}, Tree2} ->
                {Path, Tree3} = bp_tree_path:find(Key, RootId, Tree2),
                insert(Key, Value, Path, Tree3);
            {{error, not_found}, Tree2} ->
                Root = bp_tree_node:new(true),
                {{ok, RootId}, Tree3} = bp_tree_store:create_node(Root, Tree2),
                {ok, Tree4} = bp_tree_store:set_root_id(RootId, Tree3),
                {Path, Tree5} = bp_tree_path:find(Key, RootId, Tree4),
                insert(Key, Value, Path, Tree5)
        end
    catch
        _:Error -> handle_exception(Error, erlang:get_stacktrace(), Tree)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes key and associated value from a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec remove(key(), tree()) -> {ok | error() | error_stacktrace(), tree()}.
remove(Key, Tree = #bp_tree{}) ->
    remove(Key, fun(_) -> true end, Tree).

%%--------------------------------------------------------------------
%% @doc
%% Removes key and associated value from a B+ tree if predicate is satisfied.
%% @end
%%--------------------------------------------------------------------
-spec remove(key(), remove_pred(), tree()) ->
    {ok | error() | error_stacktrace(), tree()}.
remove(Key, Predicate, Tree = #bp_tree{}) ->
    try
        {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
        {Path, Tree3} = bp_tree_path:find(Key, RootId, Tree2),
        remove(Key, Predicate, Path, ?NIL, Tree3)
    catch
        _:Error -> handle_exception(Error, erlang:get_stacktrace(), Tree)
    end.

%%--------------------------------------------------------------------
%% @equiv fold({offset, 0}, Fun, Acc, Tree)
%% @end
%%--------------------------------------------------------------------
-spec fold(fold_fun(), fold_acc(), tree()) ->
    {{ok, fold_acc()} | error(), tree()}.
fold(Fun, Acc, Tree) ->
    fold({offset, 0}, Fun, Acc, Tree).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Key, Value, Acc) on successive elements of a B+ tree leaf.
%% Fun/2 must return a new accumulator, which is passed to the next call.
%% @end
%%--------------------------------------------------------------------
-spec fold(fold_init(), fold_fun(), fold_acc(), tree()) ->
    {{ok, {fold_acc(), fold_next_node_id()}} | error(), tree()}.
fold({node_id, NodeId}, Fun, Acc, Tree) ->
    case bp_tree_store:get_node(NodeId, Tree) of
        {{ok, Node}, Tree2} ->
            {{ok, fold_node(all, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end;
fold({offset, Offset}, Fun, Acc, Tree) ->
    case bp_tree_leaf:find_offset(Offset, Tree) of
        {{ok, Pos, Node}, Tree2} ->
            {{ok, fold_node({pos, Pos}, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end;
fold({start_key, Key}, Fun, Acc, Tree) ->
    case bp_tree_leaf:lower_bound_node(Key, Tree) of
        {{ok, Node}, Tree2} ->
            {{ok, fold_node({key, Key}, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end;
fold({node_of_key, Key}, Fun, Acc, Tree) ->
    case bp_tree_leaf:lower_bound_node(Key, Tree) of
        {{ok, Node}, Tree2} ->
            {{ok, fold_node(all, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end;
fold({node_prev_to_key, Key}, Fun, Acc, Tree) ->
    case get_prev_leaf({key, Key}, Tree) of
        {_, undefined, Tree2} ->
            {{error, first_node}, Tree2};
        {_, Node, Tree2} ->
            {{ok, fold_node(all, Node, Fun, Acc)}, Tree2}
    end;
% TODO - low performance
fold({prev_key, Key}, Fun, Acc, Tree) ->
    case bp_tree_leaf:find_next(Key, Tree) of
        {{ok, Pos, Node}, Tree2} ->
            {{ok, fold_node({pos, Pos}, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets previous leaf.
%% @end
%%--------------------------------------------------------------------
-spec get_prev_leaf({node, bp_tree_node:id()} | {key, key()}, tree()) ->
    {bp_tree_node:id() | undefined, tree_node() | undefined, tree()}.
get_prev_leaf({node, NodeId}, Tree) ->
    {{ok, Node}, Tree2} =  bp_tree_store:get_node(NodeId, Tree),
    get_prev_leaf(NodeId, Node, Tree2);
get_prev_leaf({key, Key}, Tree) ->
    {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
    {[{NodeId, _Node} | Path], Tree3} = bp_tree_path:find(Key, RootId, Tree2),
    get_prev_leaf(Path, NodeId, Key, Tree3).

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inserts a key-value pair into a B+ tree along the provided leaf-root path.
%% Stops when insert does not cause node split, otherwise inserts new key-value
%% pair into next node on path.
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), bp_tree_path:path(), tree()) ->
    {ok | error(), tree()}.
insert(Key, Value, [], Tree) ->
    Root = bp_tree_node:new(false),
    {ok, Root2} = bp_tree_node:insert(Key, Value, Root),
    {{ok, RootId}, Tree2} = bp_tree_store:create_node(Root2, Tree),
    bp_tree_store:set_root_id(RootId, Tree2);
insert(Key, Value, [{NodeId, Node} | Path], Tree = #bp_tree{order = Order}) ->
    {ok, Node2} = bp_tree_node:insert(Key, Value, Node),
    case bp_tree_node:size(Node2) > 2 * Order of
        true ->
            {{Key2, NewNodeId, RNodeId}, Tree2} =
                split_node(NodeId, Node2, Tree, Path),
            insert(Key2, {NewNodeId, RNodeId}, Path, Tree2);
        false ->
            bp_tree_store:update_node(NodeId, Node2, Tree)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes key and associated value from a B+ tree along the provided leaf-root
%% path with a sibling.
%% @end
%%--------------------------------------------------------------------
-spec remove(key(), remove_pred(), bp_tree_path:path() | bp_tree_path:path_with_sibling(),
    bp_tree_node:id(), tree()) -> {ok | error(), tree()}.
remove(Key, Pred, [{NodeId, Node}], ChildId, Tree) ->
    {ok, Node2} = bp_tree_node:remove(Key, Pred, Node),
    case {bp_tree_node:size(Node2), ChildId} of
        {0, ?NIL} ->
            {ok, Tree2} = bp_tree_store:delete_node(NodeId, Tree),
            bp_tree_store:unset_root_id(Tree2);
        {0, _} ->
            {ok, Tree2} = bp_tree_store:delete_node(NodeId, Tree),
            bp_tree_store:set_root_id(ChildId, Tree2);
        _ ->
            bp_tree_store:update_node(NodeId, Node2, Tree)
    end;
remove(Key, Pred, [{NodeId, Node} | _], _, Tree = #bp_tree{order = Order}) ->
    {ok, Node2} = bp_tree_node:remove(Key, Pred, Node),
    case bp_tree_node:size(Node2) < Order of
        true ->
            {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
            {Path, Tree3} = bp_tree_path:find_with_sibling(Key, RootId, Tree2),
            remove(Key, Pred, Path, ?NIL, Tree3);
        false ->
            bp_tree_store:update_node(NodeId, Node2, Tree)
    end;
remove(Key, Pred, [{{NodeId, Node}, ?NIL, ?NIL}], ChildId, Tree) ->
    remove(Key, Pred, [{NodeId, Node}], ChildId, Tree);
remove(Key, Pred, [{{NodeId, Node}, ParentKey, SNodeId} | Path], _,
    Tree = #bp_tree{order = Order}
) ->
    {ok, Node2} = bp_tree_node:remove(Key, Pred, Node),
    case bp_tree_node:size(Node2) < Order of
        true ->
            rebalance_node(Key, NodeId, Node2, ParentKey, SNodeId, Path, Tree);
        false ->
            bp_tree_store:update_node(NodeId, Node2, Tree)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits node in half.
%% @end
%%--------------------------------------------------------------------
-spec split_node(bp_tree_node:id(), tree_node(), tree(),
    bp_tree_path:path_with_sibling()) ->
    {{key(), bp_tree_node:id(), bp_tree_node:id()}, tree()}.
split_node(NodeId, Node, Tree, []) ->
    {ok, LNode, Key2, RNode} = bp_tree_node:split(Node),
    {{ok, RNodeId}, Tree2} = bp_tree_store:create_node(RNode, Tree),
    LNode2 = bp_tree_node:set_right_sibling(RNodeId, LNode),
    {{ok, NewNodeId}, Tree3} = bp_tree_store:create_node(LNode2, Tree2),
    {ok, Tree4} = bp_tree_store:delete_node(NodeId, Tree3),
    {{Key2, NewNodeId, RNodeId}, Tree4};
split_node(NodeId, Node, Tree, [{PNodeId, PNode} | PathTail]) ->
    {ok, LNode, Key2, RNode} = bp_tree_node:split(Node),
    {{ok, RNodeId}, Tree2} = bp_tree_store:create_node(RNode, Tree),
    LNode2 = bp_tree_node:set_right_sibling(RNodeId, LNode),
    {{ok, NewNodeId}, Tree3} = bp_tree_store:create_node(LNode2, Tree2),
    {ok, Tree4} = bp_tree_store:delete_node(NodeId, Tree3),

    {ok, PNode2} = bp_tree_node:insert(Key2, {NewNodeId, RNodeId}, PNode),
    NewPath = [{PNodeId, PNode2} | PathTail],
    Tree5 = update_sibling_smaller_key(NewPath, NewNodeId, Key2, Tree4,
      NewNodeId, LNode2#bp_tree_node.leaf),

    {{Key2, NewNodeId, RNodeId}, Tree5}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Rebalances a node after remove operation.
%% @end
%%--------------------------------------------------------------------
-spec rebalance_node(key(), bp_tree_node:id(), tree_node(), key(),
    bp_tree_node:id(), bp_tree_path:path_with_sibling(), tree()) ->
    {ok | error(), tree()}.
rebalance_node(Key, NodeId, Node, ParentKey, SNodeId, Path, Tree = #bp_tree{
    order = Order
}) ->
    {{ok, SNode}, Tree2} = bp_tree_store:get_node(SNodeId, Tree),
    case bp_tree_node:size(SNode) > Order of
        true when Key =< ParentKey ->
            {NewPath, NewNodeId, Node2, Tree3} = rotate_nodes(Node, SNode,
                NodeId, SNodeId, ParentKey, Tree2, Path, left),
            Tree4 = update_sibling_smaller_key(NewPath, NewNodeId, Key, Tree3,
                NewNodeId, Node2#bp_tree_node.leaf),
            {ok, Tree4};
        true ->
            {NewPath, NewNodeId, Node2, Tree3} = rotate_nodes(SNode, Node,
                SNodeId, NodeId, ParentKey, Tree2, Path, right),
            Tree4 = update_sibling_greater_key(NewPath, NewNodeId, Key, Tree3,
                NewNodeId, Node2#bp_tree_node.leaf),
            {ok, Tree4};
        false when Key =< ParentKey ->
            {NewPath, NewNodeId, Node2, Tree3} =
                merge_nodes(Node, SNode, NodeId, SNodeId, ParentKey, Tree2, Path),
            Tree4 = update_sibling_smaller_key(NewPath, NewNodeId, Key, Tree3,
                NewNodeId, Node2#bp_tree_node.leaf),
            remove(ParentKey, fun(_) -> true end, NewPath, NewNodeId, Tree4);
        false ->
            {NewPath, NewNodeId, Node2, Tree3} =
                merge_nodes(SNode, Node, SNodeId, NodeId, ParentKey, Tree2, Path),
            Tree4 = update_sibling_greater_key(NewPath, NewNodeId, Key, Tree3,
                NewNodeId, Node2#bp_tree_node.leaf),
            remove(ParentKey, fun(_) -> true end, NewPath, NewNodeId, Tree4)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merges two nodes.
%% @end
%%--------------------------------------------------------------------
-spec merge_nodes(tree_node(), tree_node(), bp_tree_node:id(), bp_tree_node:id(),
    key(), tree(), bp_tree_path:path_with_sibling()) ->
    {bp_tree_path:path_with_sibling(), bp_tree_node:id(), tree_node(), tree()}.
merge_nodes(Node, SNode, NodeId, SNodeId, ParentKey, Tree,
    [{{PNodeId, PNode}, P1, P2} | PathTail]) ->
    Node2 = bp_tree_node:merge(Node, ParentKey, SNode),
    {{ok, NewNodeId}, Tree2} = bp_tree_store:create_node(Node2, Tree),
    {ok, PNode2} = bp_tree_node:remove(ParentKey, fun(_) -> true end, PNode),
    {ok, PNode3} = bp_tree_node:insert(ParentKey, {NewNodeId, SNodeId}, PNode2),
    {ok, Tree3} = bp_tree_store:update_node(PNodeId, PNode3, Tree2),
    {ok, Tree4} = bp_tree_store:delete_node(SNodeId, Tree3),
    {ok, Tree5} = bp_tree_store:delete_node(NodeId, Tree4),
    NewPath = [{{PNodeId, PNode3}, P1, P2} | PathTail],
    {NewPath, NewNodeId, Node2, Tree5}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Rotates two nodes.
%% @end
%%--------------------------------------------------------------------
-spec rotate_nodes(tree_node(), tree_node(), bp_tree_node:id(), bp_tree_node:id(),
    key(), tree(), bp_tree_path:path_with_sibling(), left | right) ->
    {bp_tree_path:path_with_sibling(), bp_tree_node:id(), tree_node(), tree()}.
rotate_nodes(Node, SNode, NodeId, SNodeId, ParentKey, Tree, Path, Rotation) ->
    {Node2, ParentKey2, SNode2} = case Rotation of
        left ->
            bp_tree_node:rotate_left(Node, ParentKey, SNode);
        _ ->
            bp_tree_node:rotate_right(Node, ParentKey, SNode)
    end,
    {{ok, NewSNodeId}, Tree2} = bp_tree_store:create_node(SNode2, Tree),
    Node3 = case Node2#bp_tree_node.leaf of
        true ->
            bp_tree_node:set_right_sibling(NewSNodeId, Node2);
        _ ->
            Node2
    end,
    {{ok, NewNodeId}, Tree3} = bp_tree_store:create_node(Node3, Tree2),

    [{{PNodeId, PNode}, P1, P2} | PathTail] = Path,
    {ok, PNode2} = bp_tree_node:remove(ParentKey, fun(_) -> true end, PNode),
    {ok, PNode3} = bp_tree_node:insert(ParentKey2, {NewNodeId, NewSNodeId}, PNode2),
    {ok, Tree4} = bp_tree_store:update_node(PNodeId, PNode3, Tree3),

    {ok, Tree5} = bp_tree_store:delete_node(SNodeId, Tree4),
    {ok, Tree6} = bp_tree_store:delete_node(NodeId, Tree5),

    NewPath = [{{PNodeId, PNode3}, P1, P2} | PathTail],
    {NewPath, NewNodeId, Node2, Tree6}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds node left to the chosen one and set right sibling in this node.
%% @end
%%--------------------------------------------------------------------
-spec update_sibling_greater_key(bp_tree_path:path_with_sibling(),
    bp_tree_node:id(), key(), tree(), bp_tree_node:id(), boolean()) -> tree().
update_sibling_greater_key(_Path, _CheckID, _Key, Tree, _ToSet, false) ->
    Tree;
update_sibling_greater_key([{{NodeID, Node}, _, _} | PathTail], CheckID, Key,
    Tree, ToSet, IdLeaf) ->
    {ok, NextID} = bp_tree_node:leftmost_child(Node),
    case NextID of
        CheckID ->
            update_sibling_smaller_key(PathTail, NodeID, Key, Tree, ToSet, IdLeaf);
        _ ->
            case bp_tree_node:left_sibling(Key, Node) of
                {ok, NewNodeID} ->
                    {{ok, NewNode}, Tree2} = bp_tree_store:get_node(NewNodeID, Tree),
                    update_sibling(Key, NewNodeID, NewNode, Tree2, ToSet);
                _ ->
                    Tree
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds node left to the chosen one and set right sibling in this node.
%% @end
%%--------------------------------------------------------------------
-spec update_sibling_smaller_key(bp_tree_path:path() | bp_tree_path:path_with_sibling(),
    bp_tree_node:id(), key(), tree(), bp_tree_node:id(), boolean()) -> tree().
update_sibling_smaller_key(_Path, _CheckID, _Key, Tree, _ToSet, false) ->
    Tree;
update_sibling_smaller_key([], _CheckID, _Key, Tree, _ToSet, _IdLeaf) ->
    Tree;
update_sibling_smaller_key([{{NodeID, Node}, _, _} | PathTail], CheckID, Key,
    Tree, ToSet, IdLeaf) ->
    update_sibling_smaller_key([{NodeID, Node} | PathTail], CheckID, Key,
        Tree, ToSet, IdLeaf);
update_sibling_smaller_key([{NodeID, Node} | PathTail], CheckID, Key,
    Tree, ToSet, IdLeaf) ->
    {ok, NextID} = bp_tree_node:leftmost_child(Node),
    case NextID of
        CheckID ->
            update_sibling_smaller_key(PathTail, NodeID, Key, Tree, ToSet, IdLeaf);
        _ ->
            {ok, NewNodeID, _, _} = bp_tree_node:child_with_sibling(Key, Node),
            {{ok, NewNode}, Tree2} = bp_tree_store:get_node(NewNodeID, Tree),
            update_sibling(Key, NewNodeID, NewNode, Tree2, ToSet)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds node left to the chosen one and set right sibling in this node.
%% @end
%%--------------------------------------------------------------------
-spec update_sibling(key(), bp_tree_node:id(), tree_node(), tree(),
    bp_tree_node:id()) -> tree().
update_sibling(Key, NodeID, Node, Tree, ToSet) ->
    case bp_tree_node:child(Key, Node) of
        {ok, NewID} ->
            {{ok, NewNode}, Tree2} = bp_tree_store:get_node(NewID, Tree),
            update_sibling(Key, NewID, NewNode, Tree2, ToSet);
        {error, not_found} ->
            UpdatedNode = bp_tree_node:set_right_sibling(ToSet, Node),
            {ok, Tree2} = bp_tree_store:update_node(NodeID, UpdatedNode, Tree),
            Tree2
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets previous leaf.
%% @end
%%--------------------------------------------------------------------
-spec get_prev_leaf(bp_tree_node:id(), tree_node(), tree()) ->
    {bp_tree_node:id() | undefined, tree_node() | undefined, tree()}.
get_prev_leaf(NodeId, Node, Tree) ->
    {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
    {ok, Key} = bp_tree_node:key(1, Node),
    {[{NodeId, Node} | Path], Tree3} = bp_tree_path:find(Key, RootId, Tree2),
    get_prev_leaf(Path, NodeId, Key, Tree3).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets previous node previous to leaf that contains the key.
%% @end
%%--------------------------------------------------------------------
-spec get_prev_leaf(bp_tree_path:path_with_sibling(), bp_tree_node:id(), key(),
    tree()) -> {bp_tree_node:id() | undefined, tree_node() | undefined, tree()}.
get_prev_leaf([], _CheckID, _Key, Tree) ->
    {undefined, undefined, Tree};
get_prev_leaf([{NodeID, Node} | PathTail], CheckID, Key, Tree) ->
    {ok, NextID} = bp_tree_node:leftmost_child(Node),
    case NextID of
        CheckID ->
            get_prev_leaf(PathTail, NodeID, Key, Tree);
        _ ->
            {ok, NewNodeID, _, _} = bp_tree_node:child_with_sibling(Key, Node),
            {{ok, NewNode}, Tree2} = bp_tree_store:get_node(NewNodeID, Tree),
            get_prev_leaf_traverse_down(Key, NewNodeID, NewNode, Tree2)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Travers down the tree to get node previous to leaf that contains the key.
%% @end
%%--------------------------------------------------------------------
-spec get_prev_leaf_traverse_down(key(), bp_tree_node:id(), tree_node(), tree()) ->
    {bp_tree_node:id(), tree_node(), tree()}.
get_prev_leaf_traverse_down(Key, NodeID, Node, Tree) ->
    case bp_tree_node:child(Key, Node) of
        {ok, NewID} ->
            {{ok, NewNode}, Tree2} = bp_tree_store:get_node(NewID, Tree),
            get_prev_leaf_traverse_down(Key, NewID, NewNode, Tree2);
        {error, not_found} ->
            {NodeID, Node, Tree}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Folds B+ tree leaf.
%% @end
%%--------------------------------------------------------------------
-spec fold_node(fold_start_spec(), tree_node(), fold_fun(), fold_acc()) ->
    {fold_acc(), fold_next_node_id()}.
fold_node(KeyOrPos, Node, Fun, Acc) ->
    Acc2 = bp_tree_node:fold(KeyOrPos, Node, Fun, Acc),
    case bp_tree_node:right_sibling(Node) of
        {ok, NodeId} -> {Acc2, NodeId};
        {error, not_found} -> {Acc2, undefined}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles exceptions.
%% @end
%%--------------------------------------------------------------------
-spec handle_exception(term(), [erlang:stack_item()], tree()) ->
    {error() | error_stacktrace(), tree()}.
handle_exception({badmatch, {{error, Reason}, Tree = #bp_tree{}}}, _, _) ->
    {{error, Reason}, Tree};
handle_exception({badmatch, {error, Reason}}, _, Tree) ->
    {{error, Reason}, Tree};
handle_exception({badmatch, Reason}, _, Tree) ->
    {{error, Reason}, Tree};
handle_exception(Reason, Stacktrace, Tree) ->
    {{error, {Reason, Stacktrace}}, Tree}.