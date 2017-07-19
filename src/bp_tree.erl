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
-export([find/2, insert/3, remove/2, fold/3, fold/4]).

-type key() :: term().
-type value() :: term().
-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type order() :: pos_integer().
-type init_opt() :: {order, order()} |
                    {store_module, module()} |
                    {store_args, bp_tree_store:args()}.
-type fold_init() :: {start_key, key()} |
                     {prev_key, key()} |
                     {offset, non_neg_integer()}.
-type fold_acc() :: any().
-type fold_fun() :: fun((key(), value(), fold_acc()) -> fold_acc()).
-type error() :: {error, term()}.
-type error_stacktrace() :: {error, {term(), [erlang:stack_item()]}}.

-export_type([key/0, value/0, tree/0, tree_node/0, order/0]).

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
        _:{badmatch, Reason} -> {{error, Reason}, Tree};
        _:Reason -> {{error, {Reason, erlang:get_stacktrace()}}, Tree}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Inserts a key-value pair into a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), tree()) ->
    {ok | error() | error_stacktrace(), tree()}.
insert(Key, Value, Tree = #bp_tree{order = Order}) ->
    try
        case bp_tree_store:get_root_id(Tree) of
            {{ok, RootId}, Tree2} ->
                {Path, Tree3} = bp_tree_path:find(Key, RootId, Tree2),
                insert(Key, Value, Path, Tree3);
            {{error, not_found}, Tree2} ->
                Root = bp_tree_node:new(Order, true),
                {{ok, RootId}, Tree3} = bp_tree_store:create_node(Root, Tree2),
                {ok, Tree4} = bp_tree_store:set_root_id(RootId, Tree3),
                {Path, Tree5} = bp_tree_path:find(Key, RootId, Tree4),
                insert(Key, Value, Path, Tree5)
        end
    catch
        _:{badmatch, Reason} -> {{error, Reason}, Tree};
        _:Reason -> {{error, {Reason, erlang:get_stacktrace()}}, Tree}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes key and associated value from a B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec remove(key(), tree()) -> {ok | error() | error_stacktrace(), tree()}.
remove(Key, Tree = #bp_tree{}) ->
    try
        {{ok, RootId}, Tree2} = bp_tree_store:get_root_id(Tree),
        {Path, Tree3} = bp_tree_path:find_with_sibling(Key, RootId, Tree2),
        remove(Key, Path, ?NIL, Tree3)
    catch
        _:{badmatch, Reason} -> {{error, Reason}, Tree};
        _:Reason -> {{error, {Reason, erlang:get_stacktrace()}}, Tree}
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
    {{ok, fold_acc()} | error(), tree()}.
fold({offset, Offset}, Fun, Acc, Tree) ->
    case bp_tree_leaf:find_offset(Offset, Tree) of
        {{ok, Pos, Node}, Tree2} ->
            {{ok, fold_node(Pos, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end;
fold({start_key, Key}, Fun, Acc, Tree) ->
    case bp_tree_leaf:lower_bound(Key, Tree) of
        {{ok, Pos, Node}, Tree2} ->
            {{ok, fold_node(Pos, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end;
fold({prev_key, Key}, Fun, Acc, Tree) ->
    case bp_tree_leaf:find_next(Key, Tree) of
        {{ok, Pos, Node}, Tree2} ->
            {{ok, fold_node(Pos, Node, Fun, Acc)}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

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
insert(Key, Value, [], Tree = #bp_tree{order = Order}) ->
    Root = bp_tree_node:new(Order, false),
    {ok, Root2} = bp_tree_node:insert(Key, Value, Root),
    {{ok, RootId}, Tree2} = bp_tree_store:create_node(Root2, Tree),
    bp_tree_store:set_root_id(RootId, Tree2);
insert(Key, Value, [{NodeId, Node} | Path], Tree = #bp_tree{order = Order}) ->
    case bp_tree_node:insert(Key, Value, Node) of
        {ok, Node2} ->
            case bp_tree_node:size(Node2) > 2 * Order of
                true ->
                    {{Key2, RNodeId}, Tree2} = split_node(NodeId, Node2, Tree),
                    insert(Key2, {NodeId, RNodeId}, Path, Tree2);
                false ->
                    bp_tree_store:update_node(NodeId, Node2, Tree)
            end;
        {error, Reason} ->
            {{error, Reason}, Tree}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes key and associated value from a B+ tree along the provided leaf-root
%% path with a sibling.
%% @end
%%--------------------------------------------------------------------
-spec remove(key(), bp_tree_path:path_with_sibling(), bp_tree_node:id(), tree()) ->
    {ok | error(), tree()}.
remove(Key, [{{NodeId, Node}, ?NIL, ?NIL}], ChildId, Tree) ->
    case bp_tree_node:remove(Key, Node) of
        {ok, Node2} ->
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
        {error, Reason} ->
            {{error, Reason}, Tree}
    end;
remove(Key, [{{NodeId, Node}, ParentKey, SNodeId} | Path], _, Tree = #bp_tree{
    order = Order
}) ->
    {ok, Node2} = bp_tree_node:remove(Key, Node),
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
-spec split_node(bp_tree_node:id(), tree_node(), tree()) ->
    {{key(), bp_tree_node:id()}, tree()}.
split_node(NodeId, Node, Tree) ->
    {ok, LNode, Key2, RNode} = bp_tree_node:split(Node),
    {{ok, RNodeId}, Tree2} = bp_tree_store:create_node(RNode, Tree),
    LNode2 = bp_tree_node:set_right_sibling(RNodeId, LNode),
    {ok, Tree3} = bp_tree_store:update_node(NodeId, LNode2, Tree2),
    {{Key2, RNodeId}, Tree3}.

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
            {Node2, ParentKey2, SNode2} = bp_tree_node:rotate_left(Node, ParentKey, SNode),
            [{{PNodeId, PNode}, _, _} | _] = Path,
            {ok, PNode2} = bp_tree_node:replace_key(ParentKey, ParentKey2, PNode),
            {ok, Tree3} = bp_tree_store:update_node(SNodeId, SNode2, Tree2),
            {ok, Tree4} = bp_tree_store:update_node(PNodeId, PNode2, Tree3),
            {ok, _Tree5} = bp_tree_store:update_node(NodeId, Node2, Tree4);
        true ->
            {SNode2, ParentKey2, Node2} = bp_tree_node:rotate_right(SNode, ParentKey, Node),
            [{{PNodeId, PNode}, _, _} | _] = Path,
            {ok, PNode2} = bp_tree_node:replace_key(ParentKey, ParentKey2, PNode),
            {ok, Tree3} = bp_tree_store:update_node(SNodeId, SNode2, Tree2),
            {ok, Tree4} = bp_tree_store:update_node(PNodeId, PNode2, Tree3),
            {ok, _Tree5} = bp_tree_store:update_node(NodeId, Node2, Tree4);
        false when Key =< ParentKey ->
            Node2 = bp_tree_node:merge(Node, ParentKey, SNode),
            {ok, Tree3} = bp_tree_store:delete_node(SNodeId, Tree2),
            {ok, Tree4} = bp_tree_store:update_node(NodeId, Node2, Tree3),
            remove(ParentKey, Path, NodeId, Tree4);
        false ->
            SNode2 = bp_tree_node:merge(SNode, ParentKey, Node),
            {ok, Tree3} = bp_tree_store:delete_node(NodeId, Tree2),
            {ok, Tree4} = bp_tree_store:update_node(SNodeId, SNode2, Tree3),
            remove(ParentKey, Path, SNodeId, Tree4)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Folds B+ tree leaf starting from a position.
%% @end
%%--------------------------------------------------------------------
-spec fold_node(pos_integer(), tree_node(), fold_fun(), fold_acc()) ->
    fold_acc().
fold_node(Pos, Node, Fun, Acc) ->
    case {bp_tree_node:key(Pos, Node), bp_tree_node:value(Pos, Node)} of
        {{ok, Key}, {ok, Value}} ->
            Acc2 = Fun(Key, Value, Acc),
            fold_node(Pos + 1, Node, Fun, Acc2);
        {{error, out_of_range}, {error, out_of_range}} ->
            Acc
    end.
