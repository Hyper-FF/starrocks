// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for joining strings and numbers using a delimiter.
//
// Thin compatibility wrapper around absl::StrJoin.
// New code should use absl::StrJoin() directly.
//
#pragma once

#include <cstdio>
#include <cstring>
#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <map>
using std::map;
using std::multimap;
#include <set>
using std::multiset;
using std::set;
#include <string>
using std::string;
#include <utility>
using std::make_pair;
using std::pair;
#include <vector>
using std::vector;

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "gutil/integral_types.h"
#include "gutil/strings/stringpiece.h"

// ----------------------------------------------------------------------
// JoinUsing() / JoinUsingToBuffer()
//    Legacy C-string joining functions. Retained for backward compatibility.
// ----------------------------------------------------------------------
char* JoinUsing(const vector<const char*>& components, const char* delim, int* result_length_p);
char* JoinUsingToBuffer(const vector<const char*>& components, const char* delim, int result_buffer_size,
                        char* result_buffer, int* result_length_p);

// ----------------------------------------------------------------------
// JoinStrings() / JoinStringsIterator()
//    Delegates to absl::StrJoin.
// ----------------------------------------------------------------------
template <class CONTAINER>
void JoinStrings(const CONTAINER& components, const StringPiece& delim, string* result) {
    *result = absl::StrJoin(components, absl::string_view(delim.data(), delim.size()));
}

template <class CONTAINER>
string JoinStrings(const CONTAINER& components, const StringPiece& delim) {
    return absl::StrJoin(components, absl::string_view(delim.data(), delim.size()));
}

template <class ITERATOR>
void JoinStringsIterator(const ITERATOR& start, const ITERATOR& end, const StringPiece& delim, string* result) {
    *result = absl::StrJoin(start, end, absl::string_view(delim.data(), delim.size()));
}

template <class ITERATOR>
string JoinStringsIterator(const ITERATOR& start, const ITERATOR& end, const StringPiece& delim) {
    return absl::StrJoin(start, end, absl::string_view(delim.data(), delim.size()));
}

// Join the keys of a map using the specified delimiter.
template <typename ITERATOR>
void JoinKeysIterator(const ITERATOR& start, const ITERATOR& end, const StringPiece& delim, string* result) {
    *result = absl::StrJoin(start, end, absl::string_view(delim.data(), delim.size()),
                            [](string* out, const auto& kv) { absl::StrAppend(out, kv.first); });
}

template <typename ITERATOR>
string JoinKeysIterator(const ITERATOR& start, const ITERATOR& end, const StringPiece& delim) {
    string result;
    JoinKeysIterator(start, end, delim, &result);
    return result;
}

// Join the keys and values of a map using the specified delimiters.
template <typename ITERATOR>
void JoinKeysAndValuesIterator(const ITERATOR& start, const ITERATOR& end, const StringPiece& intra_delim,
                               const StringPiece& inter_delim, string* result) {
    result->clear();
    for (ITERATOR iter = start; iter != end; ++iter) {
        if (iter != start) {
            result->append(inter_delim.data(), inter_delim.size());
        }
        absl::StrAppend(result, iter->first);
        result->append(intra_delim.data(), intra_delim.size());
        absl::StrAppend(result, iter->second);
    }
}

template <typename ITERATOR>
string JoinKeysAndValuesIterator(const ITERATOR& start, const ITERATOR& end, const StringPiece& intra_delim,
                                 const StringPiece& inter_delim) {
    string result;
    JoinKeysAndValuesIterator(start, end, intra_delim, inter_delim, &result);
    return result;
}

void JoinStringsInArray(string const* const* components, int num_components, const char* delim, string* result);
void JoinStringsInArray(string const* components, int num_components, const char* delim, string* result);

inline string JoinStringsInArray(string const* const* components, int num_components, const char* delim) {
    string result;
    JoinStringsInArray(components, num_components, delim, &result);
    return result;
}

inline string JoinStringsInArray(string const* components, int num_components, const char* delim) {
    string result;
    JoinStringsInArray(components, num_components, delim, &result);
    return result;
}

// JoinMapKeysAndValues / JoinVectorKeysAndValues
void JoinMapKeysAndValues(const map<string, string>& components, const StringPiece& intra_delim,
                          const StringPiece& inter_delim, string* result);
void JoinVectorKeysAndValues(const vector<pair<string, string> >& components, const StringPiece& intra_delim,
                             const StringPiece& inter_delim, string* result);

template <typename T>
void JoinHashMapKeysAndValues(const T& container, const StringPiece& intra_delim, const StringPiece& inter_delim,
                              string* result) {
    JoinKeysAndValuesIterator(container.begin(), container.end(), intra_delim, inter_delim, result);
}

// JoinCSVLine
void JoinCSVLine(const vector<string>& original_cols, string* output);
string JoinCSVLine(const vector<string>& original_cols);
void JoinCSVLineWithDelimiter(const vector<string>& original_cols, char delimiter, string* output);

// Join the strings produced by calling 'functor' on each element.
template <class CONTAINER, typename FUNC>
string JoinMapped(const CONTAINER& components, const FUNC& functor, const StringPiece& delim) {
    string result;
    for (auto iter = components.begin(); iter != components.end(); iter++) {
        if (iter != components.begin()) {
            result.append(delim.data(), delim.size());
        }
        result.append(functor(*iter));
    }
    return result;
}

// JoinElements / JoinElementsIterator - delegate to absl::StrJoin
template <class ITERATOR>
void JoinElementsIterator(ITERATOR first, ITERATOR last, StringPiece delim, string* result) {
    *result = absl::StrJoin(first, last, absl::string_view(delim.data(), delim.size()));
}

template <class ITERATOR>
string JoinElementsIterator(ITERATOR first, ITERATOR last, StringPiece delim) {
    return absl::StrJoin(first, last, absl::string_view(delim.data(), delim.size()));
}

template <class CONTAINER>
inline void JoinElements(const CONTAINER& components, StringPiece delim, string* result) {
    JoinElementsIterator(components.begin(), components.end(), delim, result);
}

template <class CONTAINER>
inline string JoinElements(const CONTAINER& components, StringPiece delim) {
    return JoinElementsIterator(components.begin(), components.end(), delim);
}

template <class CONTAINER>
void JoinInts(const CONTAINER& components, const char* delim, string* result) {
    JoinElements(components, delim, result);
}

template <class CONTAINER>
inline string JoinInts(const CONTAINER& components, const char* delim) {
    return JoinElements(components, delim);
}
