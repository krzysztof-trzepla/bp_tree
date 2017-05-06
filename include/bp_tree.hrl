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
    degree,
    root
}).

-record(bp_node, {
    leaf,
    children
}).

-endif.