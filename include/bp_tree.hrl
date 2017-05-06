%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BP_TREE_HRL).
-define(BP_TREE_HRL, 1).

-record(bp_tree, {
    degree :: pos_integer(),
    root :: bp_tree_node:id(),
    store_module :: module(),
    store_state :: bp_tree_store:state()
}).

-record(bp_tree_node, {
    leaf :: boolean(),
    parent :: undefined | bp_tree_node:id(),
    children :: bp_tree_node:children()
}).

-endif.