#ifndef BOOST_PROPERTY_TREE_WRAPPER_H
#define BOOST_PROPERTY_TREE_WRAPPER_H

/**
 * Boost property tree is not thread safe
 * https://stackoverflow.com/questions/8156948/is-boostproperty-treeptree-thread-safe
 * 
 * You have to define macro BOOST_SPIRIT_THREADSAFE to make it so !
 * Creating this common file ensures that everyone using property tree
 * gets the threadsafe version
 */

#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/exceptions.hpp>
#include <boost/property_tree/json_parser.hpp>

#endif
