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
-export([new/0, new/1]).
-export([find/2, insert/3, remove/2, fold/3, fold/4]).

-record(fold_token, {
    function :: fold_fun(),
    pos :: undefined | pos_integer(),
    node_id :: undefined | bp_tree_node:id(),
    start_key :: undefined | key(),
    prev_key :: undefined | key(),
    end_key :: undefined | key(),
    total_ctr = 0 :: non_neg_integer(),
    total_size :: undefined | non_neg_integer(),
    batch_ctr = 0 :: non_neg_integer(),
    batch_size :: undefined | pos_integer()
}).

-type key() :: term().
-type value() :: term().
-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type order() :: pos_integer().
-type new_opt() :: {order, order()} |
                   {store_module, module()} |
                   {store_args, bp_tree_store:args()}.
-opaque fold_token() :: #fold_token{}.
-type fold_opt() :: {start_key, key()} |
                    {end_key, key()} |
                    {offset, non_neg_integer()} |
                    {total_size, non_neg_integer()} |
                    {batch_size, pos_integer()}.
-type fold_acc() :: any().
-type fold_fun() :: fun((key(), value(), fold_acc()) -> fold_acc()).
-type error() :: {error, term()}.
-type error_stacktrace() :: {error, term(), [erlang:stack_item()]}.

-export_type([key/0, value/0, tree/0, tree_node/0, order/0, fold_token/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @equiv new([])
%% @end
%%--------------------------------------------------------------------
-spec new() -> {ok, tree()} | error().
new() ->
    new([]).

%%--------------------------------------------------------------------
%% @doc
%% Creates new B+ tree.
%% @end
%%--------------------------------------------------------------------
-spec new([new_opt()]) -> {ok, tree()} | error().
new(Opts) ->
    Order = proplists:get_value(order, Opts, 50),
    Module = proplists:get_value(store_module, Opts, bp_tree_map_store),
    Args = proplists:get_value(store_args, Opts, []),
    case Module:init(Args) of
        {ok, State} ->
            {ok, #bp_tree{
                order = Order,
                store_module = Module,
                store_state = State
            }};
        {error, Reason} ->
            {error, Reason}
    end.

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
        _:Reason -> {{error, Reason, erlang:get_stacktrace()}, Tree}
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
        _:Reason -> {{error, Reason, erlang:get_stacktrace()}, Tree}
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
        _:Reason -> {{error, Reason, erlang:get_stacktrace()}, Tree}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Folds B+ tree leafs using continuation token.
%% @end
%%--------------------------------------------------------------------
-spec fold(fold_token(), fold_acc(), tree()) ->
    {ok, fold_acc(), tree()} | {continue, fold_token(), fold_acc(), tree()}.
fold(Token = #fold_token{start_key = undefined}, Acc, Tree = #bp_tree{}) ->
    case bp_tree_leaf:find_leftmost(Tree) of
        {{ok, NodeId, Node}, Tree2} ->
            {ok, Key} = bp_tree_node:key(1, Node),
            Token2 = Token#fold_token{
                pos = 1,
                node_id = NodeId,
                start_key = Key
            },
            fold_node(Token2, Acc, Node, Tree2);
        {{error, not_found}, Tree2} ->
            {ok, Acc, Tree2}
    end;
fold(Token = #fold_token{node_id = undefined}, Acc, Tree = #bp_tree{}) ->
    case bp_tree_store:get_root_id(Tree) of
        {{ok, RootId}, Tree2} ->
            #fold_token{start_key = Key} = Token,
            {[{NodeId, Node} | _], Tree3} = bp_tree_path:find(Key, RootId, Tree2),
            Token2 = Token#fold_token{node_id = NodeId},
            fold_node(Token2, Acc, Node, Tree3);
        {{error, not_found}, Tree2} ->
            {ok, Acc, Tree2}
    end;
fold(Token = #fold_token{node_id = NodeId}, Acc, Tree = #bp_tree{}) ->
    {{ok, Node}, Tree2} = bp_tree_store:get_node(NodeId, Tree),
    fold_node(Token, Acc, Node, Tree2).

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Key, Value, Acc) on successive leafs of B+ tree. Fun/2 must
%% return a new accumulator, which is passed to the next call. The function
%% returns the final value of the accumulator and for batch fold a continuation
%% token which may be passed to {@link fold/3}.
%% @end
%%--------------------------------------------------------------------
-spec fold(fold_fun(), fold_acc(), tree(), [fold_opt()]) ->
    {ok, fold_acc(), tree()} | {continue, fold_token(), fold_acc(), tree()}.
fold(Fun, Acc, Tree = #bp_tree{}, Opts) ->
    Token = #fold_token{
        function = Fun,
        start_key = proplists:get_value(start_key, Opts),
        end_key = proplists:get_value(end_key, Opts),
        total_size = proplists:get_value(total_size, Opts),
        batch_size = proplists:get_value(batch_size, Opts)
    },
    case proplists:get_value(offset, Opts) of
        undefined ->
            fold(Token, Acc, Tree);
        Offset ->
            case bp_tree_leaf:find_key(Offset, Tree) of
                {{ok, Key, Pos, NodeId, Node}, Tree2} ->
                    Token2 = Token#fold_token{
                        pos = Pos,
                        start_key = Key,
                        node_id = NodeId
                    },
                    fold_node(Token2, Acc, Node, Tree2);
                {{error, not_found}, Tree2} ->
                    {ok, Acc, Tree2}
            end
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
%% @private
%% @doc
%% Folds B+ tree leafs.
%% @end
%%--------------------------------------------------------------------
-spec fold_node(fold_token(), fold_acc(), tree_node(), tree()) ->
    {ok, fold_acc(), tree()} | {continue, fold_token(), fold_acc(), tree()}.
fold_node(#fold_token{total_ctr = Ctr, total_size = Size}, Acc, _, Tree)
    when Ctr >= Size ->
    {ok, Acc, Tree};
fold_node(Token = #fold_token{batch_ctr = Ctr, batch_size = Size}, Acc, _, Tree)
    when Ctr >= Size ->
    {continue, Token#fold_token{batch_ctr = 0}, Acc, Tree};
fold_node(Token = #fold_token{pos = undefined}, Acc, Node, Tree) ->
    Pos = bp_tree_node:lower_bound(Token#fold_token.start_key, Node),
    fold_node(Token#fold_token{pos = Pos}, Acc, Node, Tree);
fold_node(Token = #fold_token{
    function = Fun,
    pos = Pos,
    prev_key = PrevKey,
    end_key = EndKey,
    total_ctr = TotalCtr,
    batch_ctr = BatchCtr
}, Acc, Node, Tree) ->
    case bp_tree_node:key(Pos, Node) of
        {ok, Key} when Key > EndKey ->
            {ok, Acc, Tree};
        {ok, Key} when PrevKey =/= undefined andalso Key =< PrevKey ->
            fold_node(Token#fold_token{pos = Pos + 1}, Acc, Node, Tree);
        {ok, Key} ->
            {ok, Value} = bp_tree_node:value(Pos, Node),
            Token2 = Token#fold_token{
                pos = Pos + 1,
                prev_key = Key,
                total_ctr = TotalCtr + 1,
                batch_ctr = BatchCtr + 1
            },
            fold_node(Token2, Fun(Key, Value, Acc), Node, Tree);
        {error, out_of_range} ->
            case bp_tree_node:right_sibling(Node) of
                {ok, NodeId2} ->
                    {{ok, Node2}, Tree2} = bp_tree_store:get_node(NodeId2, Tree),
                    Token2 = Token#fold_token{node_id = NodeId2, pos = 1},
                    fold_node(Token2, Acc, Node2, Tree2);
                {error, not_found} ->
                    {ok, Acc, Tree}
            end
    end.