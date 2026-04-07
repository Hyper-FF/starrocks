// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for splitting strings into substrings.
//
// This file provides string splitting via absl::StrSplit() with
// backward-compatible aliases in the strings:: namespace.
// New code should use absl::StrSplit() directly.
//
#pragma once

#include <algorithm>
#include <cstddef>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
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
#include <unordered_map>
#include <unordered_set>

#include "absl/strings/str_split.h"
#include "gutil/integral_types.h"
#include "gutil/logging-inl.h"
#include "gutil/logging.h"
#include "gutil/strings/charset.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/strip.h"

namespace strings {

//                              strings::Split()
//
// Thin wrapper around absl::StrSplit(). See absl/strings/str_split.h
// for full documentation.
//
// Example:
//   vector<string> v = strings::Split("a,b,c", ",");
//   vector<string> v = strings::Split("a,,b", ",", SkipEmpty());

template <typename Delimiter>
auto Split(absl::string_view text, Delimiter d) {
    return absl::StrSplit(text, d);
}

template <typename Delimiter, typename Predicate>
auto Split(absl::string_view text, Delimiter d, Predicate p) {
    return absl::StrSplit(text, d, p);
}

// Overloads for string-like delimiters (implicitly become ByString).
inline auto Split(absl::string_view text, const char* delimiter) {
    return absl::StrSplit(text, delimiter);
}

inline auto Split(absl::string_view text, const string& delimiter) {
    return absl::StrSplit(text, delimiter);
}

inline auto Split(absl::string_view text, absl::string_view delimiter) {
    return absl::StrSplit(text, absl::ByString(delimiter));
}

template <typename Predicate>
auto Split(absl::string_view text, const char* delimiter, Predicate p) {
    return absl::StrSplit(text, delimiter, p);
}

template <typename Predicate>
auto Split(absl::string_view text, const string& delimiter, Predicate p) {
    return absl::StrSplit(text, delimiter, p);
}

template <typename Predicate>
auto Split(absl::string_view text, absl::string_view delimiter, Predicate p) {
    return absl::StrSplit(text, absl::ByString(delimiter), p);
}

namespace delimiter {

// Backward-compatible delimiter aliases.
// strings::delimiter::Literal  → absl::ByString
// strings::delimiter::AnyOf    → absl::ByAnyChar
// strings::delimiter::Limit    → absl::MaxSplits

using Literal = absl::ByString;
using AnyOf = absl::ByAnyChar;

template <typename Delimiter>
auto Limit(Delimiter delim, int limit) {
    return absl::MaxSplits(std::move(delim), limit);
}

inline auto Limit(const char* s, int limit) {
    return absl::MaxSplits(absl::ByString(s), limit);
}

inline auto Limit(const string& s, int limit) {
    return absl::MaxSplits(absl::ByString(s), limit);
}

inline auto Limit(absl::string_view s, int limit) {
    return absl::MaxSplits(absl::ByString(std::string(s)), limit);
}

} // namespace delimiter

// Predicates - forward to absl equivalents.
using ::absl::AllowEmpty;
using ::absl::SkipEmpty;
using ::absl::SkipWhitespace;

} // namespace strings

//
// ==================== LEGACY SPLIT FUNCTIONS ====================
//

/* @defgroup SplitFunctions
 * @{ */

void ClipString(char* str, int max_len);
void ClipString(string* full_str, int max_len);
void SplitStringToLines(const char* full, int max_len, int num_lines, vector<string>* result);
string SplitOneStringToken(const char** source, const char* delim);
vector<char*>* SplitUsing(char* full, const char* delimiters);
void SplitToVector(char* full, const char* delimiters, vector<char*>* vec, bool omit_empty_strings);
void SplitToVector(char* full, const char* delimiters, vector<const char*>* vec, bool omit_empty_strings);
void SplitStringPieceToVector(const StringPiece& full, const char* delim, vector<StringPiece>* vec,
                              bool omit_empty_strings);
void SplitStringUsing(const string& full, const char* delimiters, vector<string>* result);
void SplitStringToHashsetUsing(const string& full, const char* delimiters, std::unordered_set<string>* result);
void SplitStringToSetUsing(const string& full, const char* delimiters, set<string>* result);
void SplitStringToMapUsing(const string& full, const char* delim, map<string, string>* result);
void SplitStringToHashmapUsing(const string& full, const char* delim, std::unordered_map<string, string>* result);
void SplitStringAllowEmpty(const string& full, const char* delim, vector<string>* result);
void SplitStringWithEscaping(const string& full, const strings::CharSet& delimiters, vector<string>* result);
void SplitStringWithEscapingAllowEmpty(const string& full, const strings::CharSet& delimiters, vector<string>* result);
void SplitStringWithEscapingToSet(const string& full, const strings::CharSet& delimiters, set<string>* result);
void SplitStringWithEscapingToHashset(const string& full, const strings::CharSet& delimiters,
                                      std::unordered_set<string>* result);
void SplitStringIntoNPiecesAllowEmpty(const string& full, const char* delimiters, int pieces, vector<string>* result);

template <class T>
bool SplitStringAndParse(StringPiece source, StringPiece delim, bool (*parse)(const string& str, T* value),
                         vector<T>* result);
template <class Container>
bool SplitStringAndParseToContainer(StringPiece source, StringPiece delim,
                                    bool (*parse)(const string& str, typename Container::value_type* value),
                                    Container* result);

template <class List>
bool SplitStringAndParseToList(StringPiece source, StringPiece delim,
                               bool (*parse)(const string& str, typename List::value_type* value), List* result);

bool SplitRange(const char* rangestr, int* from, int* to);

void SplitCSVLine(char* line, vector<char*>* cols);
void SplitCSVLineWithDelimiter(char* line, char delimiter, vector<char*>* cols);
void SplitCSVLineWithDelimiterForStrings(const string& line, char delimiter, vector<string>* cols);
char* SplitStructuredLine(char* line, char delimiter, const char* symbol_pairs, vector<char*>* cols);
bool SplitStructuredLine(StringPiece line, char delimiter, const char* symbol_pairs, vector<StringPiece>* cols);
char* SplitStructuredLineWithEscapes(char* line, char delimiter, const char* symbol_pairs, vector<char*>* cols);
bool SplitStructuredLineWithEscapes(StringPiece line, char delimiter, const char* symbol_pairs,
                                    vector<StringPiece>* cols);

bool SplitStringIntoKeyValues(const string& line, const string& key_value_delimiters,
                              const string& value_value_delimiters, string* key, vector<string>* values);
bool SplitStringIntoKeyValuePairs(const string& line, const string& key_value_delimiters,
                                  const string& key_value_pair_delimiters, vector<pair<string, string> >* kv_pairs);

const char* SplitLeadingDec32Values(const char* next, vector<int32>* result);
const char* SplitLeadingDec64Values(const char* next, vector<int64>* result);

bool SplitOneIntToken(const char** source, const char* delim, int* value);
bool SplitOneInt32Token(const char** source, const char* delim, int32* value);
bool SplitOneUint32Token(const char** source, const char* delim, uint32* value);
bool SplitOneInt64Token(const char** source, const char* delim, int64* value);
bool SplitOneUint64Token(const char** source, const char* delim, uint64* value);
bool SplitOneDoubleToken(const char** source, const char* delim, double* value);
bool SplitOneFloatToken(const char** source, const char* delim, float* value);

inline bool SplitOneUInt32Token(const char** source, const char* delim, uint32* value) {
    return SplitOneUint32Token(source, delim, value);
}

inline bool SplitOneUInt64Token(const char** source, const char* delim, uint64* value) {
    return SplitOneUint64Token(source, delim, value);
}

bool SplitOneDecimalIntToken(const char** source, const char* delim, int* value);
bool SplitOneDecimalInt32Token(const char** source, const char* delim, int32* value);
bool SplitOneDecimalUint32Token(const char** source, const char* delim, uint32* value);
bool SplitOneDecimalInt64Token(const char** source, const char* delim, int64* value);
bool SplitOneDecimalUint64Token(const char** source, const char* delim, uint64* value);
bool SplitOneHexUint32Token(const char** source, const char* delim, uint32* value);
bool SplitOneHexUint64Token(const char** source, const char* delim, uint64* value);

// ###################### TEMPLATE INSTANTIATIONS BELOW #######################

template <class T>
bool SplitStringAndParse(StringPiece source, StringPiece delim, bool (*parse)(const string& str, T* value),
                         vector<T>* result) {
    return SplitStringAndParseToList(source, delim, parse, result);
}

namespace strings {
namespace internal {

template <class Container, class InsertPolicy>
bool SplitStringAndParseToInserter(StringPiece source, StringPiece delim,
                                   bool (*parse)(const string& str, typename Container::value_type* value),
                                   Container* result, InsertPolicy insert_policy) {
    CHECK(nullptr != parse);
    CHECK(nullptr != result);
    CHECK(nullptr != delim.data());
    CHECK_GT(delim.size(), 0);
    bool retval = true;
    absl::string_view src(source.data(), source.size());
    absl::string_view dlm(delim.data(), delim.size());
    std::vector<std::string> pieces = absl::StrSplit(src, absl::ByAnyChar(dlm), absl::SkipEmpty());
    for (const auto& piece : pieces) {
        typename Container::value_type t;
        if (parse(piece, &t)) {
            insert_policy(result, t);
        } else {
            retval = false;
        }
    }
    return retval;
}

struct BasicInsertPolicy {
    template <class C, class V>
    void operator()(C* c, const V& v) const {
        c->insert(v);
    }
};

struct BackInsertPolicy {
    template <class C, class V>
    void operator()(C* c, const V& v) const {
        c->push_back(v);
    }
};

} // namespace internal
} // namespace strings

template <class Container>
bool SplitStringAndParseToContainer(StringPiece source, StringPiece delim,
                                    bool (*parse)(const string& str, typename Container::value_type* value),
                                    Container* result) {
    return strings::internal::SplitStringAndParseToInserter(source, delim, parse, result,
                                                            strings::internal::BasicInsertPolicy());
}

template <class List>
bool SplitStringAndParseToList(StringPiece source, StringPiece delim,
                               bool (*parse)(const string& str, typename List::value_type* value), List* result) {
    return strings::internal::SplitStringAndParseToInserter(source, delim, parse, result,
                                                            strings::internal::BackInsertPolicy());
}

/* @} */
