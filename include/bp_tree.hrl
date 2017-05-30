%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header contains common macros and records definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BP_TREE_HRL).
-define(BP_TREE_HRL, 1).

-define(NIL, nil).

-record(bp_tree, {
    order :: bp_tree:order(),
    store_module :: module(),
    store_state :: bp_tree_store:state()
}).

-record(bp_tree_node, {
    leaf :: boolean(),
    children :: bp_tree_array:array()
}).

-endif.