// Copyright 2008 and onwards Google Inc.  All rights reserved.
//
// Maintainer: Greg Miller <jgm@google.com>
//
// Implementation migrated to absl::StrSplit (see split.h).
// This file retains only legacy split functions.

#include "gutil/strings/split.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <limits>
using std::numeric_limits;

using std::unordered_map;
using std::unordered_set;

#include "absl/strings/str_split.h"
#include "gutil/integral_types.h"
#include "gutil/logging.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/util.h"
#include "gutil/strtoint.h"

// Constants for ClipString()
static const int kMaxOverCut = 12;
static const char kCutStr[] = "...";
static const int kCutStrSize = sizeof(kCutStr) - 1;

static int ClipStringHelper(const char* str, int max_len, bool use_ellipsis) {
    if (strlen(str) <= max_len) return -1;

    int max_substr_len = max_len;
    if (use_ellipsis && max_len > kCutStrSize) {
        max_substr_len -= kCutStrSize;
    }

    const char* cut_by = (max_substr_len < kMaxOverCut ? str : str + max_len - kMaxOverCut);
    const char* cut_at = str + max_substr_len;
    while (!ascii_isspace(*cut_at) && cut_at > cut_by) cut_at--;

    if (cut_at == cut_by) {
        return max_substr_len;
    } else {
        return cut_at - str;
    }
}

void ClipString(char* str, int max_len) {
    int cut_at = ClipStringHelper(str, max_len, true);
    if (cut_at != -1) {
        if (max_len > kCutStrSize) {
            strcpy(str + cut_at, kCutStr);
        } else {
            strcpy(str + cut_at, "");
        }
    }
}

void ClipString(string* full_str, int max_len) {
    int cut_at = ClipStringHelper(full_str->c_str(), max_len, true);
    if (cut_at != -1) {
        full_str->erase(cut_at);
        if (max_len > kCutStrSize) {
            full_str->append(kCutStr);
        }
    }
}

// ----------------------------------------------------------------------
// SplitStringToIteratorAllowEmpty()
// ----------------------------------------------------------------------
template <typename StringType, typename ITR>
static inline void SplitStringToIteratorAllowEmpty(const StringType& full, const char* delim, int pieces, ITR& result) {
    string::size_type begin_index, end_index;
    begin_index = 0;

    for (int i = 0; (i < pieces - 1) || (pieces == 0); i++) {
        end_index = full.find_first_of(delim, begin_index);
        if (end_index == string::npos) {
            *result++ = full.substr(begin_index);
            return;
        }
        *result++ = full.substr(begin_index, (end_index - begin_index));
        begin_index = end_index + 1;
    }
    *result++ = full.substr(begin_index);
}

void SplitStringIntoNPiecesAllowEmpty(const string& full, const char* delim, int pieces, vector<string>* result) {
    if (pieces == 0) {
        // No limit when pieces is 0.
        vector<string> v = absl::StrSplit(full, absl::ByAnyChar(delim));
        result->insert(result->end(), v.begin(), v.end());
    } else {
        int limit = std::max(pieces - 1, 0);
        vector<string> v = absl::StrSplit(full, absl::MaxSplits(absl::ByAnyChar(delim), limit));
        result->insert(result->end(), v.begin(), v.end());
    }
}

void SplitStringAllowEmpty(const string& full, const char* delim, vector<string>* result) {
    vector<string> v = absl::StrSplit(full, absl::ByAnyChar(delim));
    result->insert(result->end(), v.begin(), v.end());
}

// If we know how much to allocate for a vector of strings, we can
// allocate the vector<string> only once and directly to the right size.
static int CalculateReserveForVector(const string& full, const char* delim) {
    int count = 0;
    if (delim[0] != '\0' && delim[1] == '\0') {
        char c = delim[0];
        const char* p = full.data();
        const char* end = p + full.size();
        while (p != end) {
            if (*p == c) {
                ++p;
            } else {
                while (++p != end && *p != c) {
                }
                ++count;
            }
        }
    }
    return count;
}

template <typename StringType, typename ITR>
static inline void SplitStringToIteratorUsing(const StringType& full, const char* delim, ITR& result) {
    if (delim[0] != '\0' && delim[1] == '\0') {
        char c = delim[0];
        const char* p = full.data();
        const char* end = p + full.size();
        while (p != end) {
            if (*p == c) {
                ++p;
            } else {
                const char* start = p;
                while (++p != end && *p != c) {
                }
                *result++ = StringType(start, p - start);
            }
        }
        return;
    }

    string::size_type begin_index, end_index;
    begin_index = full.find_first_not_of(delim);
    while (begin_index != string::npos) {
        end_index = full.find_first_of(delim, begin_index);
        if (end_index == string::npos) {
            *result++ = full.substr(begin_index);
            return;
        }
        *result++ = full.substr(begin_index, (end_index - begin_index));
        begin_index = full.find_first_not_of(delim, end_index);
    }
}

void SplitStringUsing(const string& full, const char* delim, vector<string>* result) {
    result->reserve(result->size() + CalculateReserveForVector(full, delim));
    std::back_insert_iterator<vector<string> > it(*result);
    SplitStringToIteratorUsing(full, delim, it);
}

void SplitStringToHashsetUsing(const string& full, const char* delim, unordered_set<string>* result) {
    vector<string> v = absl::StrSplit(full, absl::ByAnyChar(delim), absl::SkipEmpty());
    result->insert(v.begin(), v.end());
}

void SplitStringToSetUsing(const string& full, const char* delim, set<string>* result) {
    vector<string> v = absl::StrSplit(full, absl::ByAnyChar(delim), absl::SkipEmpty());
    result->insert(v.begin(), v.end());
}

void SplitStringToMapUsing(const string& full, const char* delim, map<string, string>* result) {
    map<string, string> tmp = absl::StrSplit(full, absl::ByAnyChar(delim), absl::SkipEmpty());
    for (auto& kv : tmp) {
        (*result)[kv.first] = kv.second;
    }
}

void SplitStringToHashmapUsing(const string& full, const char* delim, unordered_map<string, string>* result) {
    // absl::StrSplit doesn't directly convert to unordered_map, use vector of pairs
    vector<string> v = absl::StrSplit(full, absl::ByAnyChar(delim), absl::SkipEmpty());
    for (size_t i = 0; i + 1 < v.size(); i += 2) {
        (*result)[v[i]] = v[i + 1];
    }
    if (v.size() % 2 == 1) {
        (*result)[v.back()] = "";
    }
}

void SplitStringPieceToVector(const StringPiece& full, const char* delim, vector<StringPiece>* vec,
                              bool omit_empty_strings) {
    absl::string_view sv(full.data(), full.size());
    if (omit_empty_strings) {
        for (absl::string_view piece : absl::StrSplit(sv, absl::ByAnyChar(delim), absl::SkipEmpty())) {
            vec->push_back(StringPiece(piece.data(), piece.size()));
        }
    } else {
        for (absl::string_view piece : absl::StrSplit(sv, absl::ByAnyChar(delim))) {
            vec->push_back(StringPiece(piece.data(), piece.size()));
        }
    }
}

vector<char*>* SplitUsing(char* full, const char* delim) {
    auto vec = new vector<char*>;
    SplitToVector(full, delim, vec, true);
    return vec;
}

void SplitToVector(char* full, const char* delim, vector<char*>* vec, bool omit_empty_strings) {
    char* next = full;
    while ((next = gstrsep(&full, delim)) != nullptr) {
        if (omit_empty_strings && next[0] == '\0') continue;
        vec->push_back(next);
    }
    if (full != nullptr) {
        vec->push_back(full);
    }
}

void SplitToVector(char* full, const char* delim, vector<const char*>* vec, bool omit_empty_strings) {
    char* next = full;
    while ((next = gstrsep(&full, delim)) != nullptr) {
        if (omit_empty_strings && next[0] == '\0') continue;
        vec->push_back(next);
    }
    if (full != nullptr) {
        vec->push_back(full);
    }
}

string SplitOneStringToken(const char** source, const char* delim) {
    assert(source);
    assert(delim);
    if (!*source) {
        return {};
    }
    const char* begin = *source;
    if (delim[0] != '\0' && delim[1] == '\0') {
        *source = strchr(*source, delim[0]);
    } else {
        *source = strpbrk(*source, delim);
    }
    if (*source) {
        return string(begin, (*source)++);
    } else {
        return string(begin);
    }
}

// ----------------------------------------------------------------------
// SplitStringWithEscaping()
// ----------------------------------------------------------------------
template <typename ITR>
static inline void SplitStringWithEscapingToIterator(const string& src, const strings::CharSet& delimiters,
                                                     const bool allow_empty, ITR* result) {
    CHECK(!delimiters.Test('\\')) << "\\ is not allowed as a delimiter.";
    CHECK(result);
    string part;

    for (uint32 i = 0; i < src.size(); ++i) {
        char current_char = src[i];
        if (delimiters.Test(current_char)) {
            if (allow_empty || !part.empty()) {
                *(*result)++ = part;
                part.clear();
            }
        } else if (current_char == '\\' && ++i < src.size()) {
            current_char = src[i];
            if (current_char != '\\' && !delimiters.Test(current_char)) {
                part.push_back('\\');
            }
            part.push_back(current_char);
        } else {
            part.push_back(current_char);
        }
    }

    if (allow_empty || !part.empty()) {
        *(*result)++ = part;
    }
}

void SplitStringWithEscaping(const string& full, const strings::CharSet& delimiters, vector<string>* result) {
    std::back_insert_iterator<vector<string> > it(*result);
    SplitStringWithEscapingToIterator(full, delimiters, false, &it);
}

void SplitStringWithEscapingAllowEmpty(const string& full, const strings::CharSet& delimiters, vector<string>* result) {
    std::back_insert_iterator<vector<string> > it(*result);
    SplitStringWithEscapingToIterator(full, delimiters, true, &it);
}

void SplitStringWithEscapingToSet(const string& full, const strings::CharSet& delimiters, set<string>* result) {
    std::insert_iterator<set<string> > it(*result, result->end());
    SplitStringWithEscapingToIterator(full, delimiters, false, &it);
}

void SplitStringWithEscapingToHashset(const string& full, const strings::CharSet& delimiters,
                                      unordered_set<string>* result) {
    std::insert_iterator<unordered_set<string> > it(*result, result->end());
    SplitStringWithEscapingToIterator(full, delimiters, false, &it);
}

// ----------------------------------------------------------------------
// SplitOne*Token()
// ----------------------------------------------------------------------
static inline long strto32_0(const char* source, char** end) {
    return strto32(source, end, 0);
}
static inline unsigned long strtou32_0(const char* source, char** end) {
    return strtou32(source, end, 0);
}
static inline int64 strto64_0(const char* source, char** end) {
    return strto64(source, end, 0);
}
static inline uint64 strtou64_0(const char* source, char** end) {
    return strtou64(source, end, 0);
}
static inline long strto32_10(const char* source, char** end) {
    return strto32(source, end, 10);
}
static inline unsigned long strtou32_10(const char* source, char** end) {
    return strtou32(source, end, 10);
}
static inline int64 strto64_10(const char* source, char** end) {
    return strto64(source, end, 10);
}
static inline uint64 strtou64_10(const char* source, char** end) {
    return strtou64(source, end, 10);
}
static inline uint32 strtou32_16(const char* source, char** end) {
    return strtou32(source, end, 16);
}
static inline uint64 strtou64_16(const char* source, char** end) {
    return strtou64(source, end, 16);
}

#define DEFINE_SPLIT_ONE_NUMBER_TOKEN(name, type, function)                           \
    bool SplitOne##name##Token(const char** source, const char* delim, type* value) { \
        assert(source);                                                               \
        assert(delim);                                                                \
        assert(value);                                                                \
        if (!*source) return false;                                                   \
        char* end;                                                                    \
        *value = function(*source, &end);                                             \
        if (end == *source) return false;                                             \
        if (end[0] && !strchr(delim, end[0])) return false;                           \
        if (*end != '\0')                                                             \
            *source = const_cast<const char*>(end + 1);                               \
        else                                                                          \
            *source = NULL;                                                           \
        return true;                                                                  \
    }

DEFINE_SPLIT_ONE_NUMBER_TOKEN(Int, int, strto32_0)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Int32, int32, strto32_0)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Uint32, uint32, strtou32_0)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Int64, int64, strto64_0)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Uint64, uint64, strtou64_0)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Double, double, strtod)
#ifdef _MSC_VER
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Float, float, strtod)
#else
DEFINE_SPLIT_ONE_NUMBER_TOKEN(Float, float, strtof)
#endif
DEFINE_SPLIT_ONE_NUMBER_TOKEN(DecimalInt, int, strto32_10)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(DecimalInt32, int32, strto32_10)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(DecimalUint32, uint32, strtou32_10)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(DecimalInt64, int64, strto64_10)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(DecimalUint64, uint64, strtou64_10)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(HexUint32, uint32, strtou32_16)
DEFINE_SPLIT_ONE_NUMBER_TOKEN(HexUint64, uint64, strtou64_16)

// ----------------------------------------------------------------------
// SplitRange()
// ----------------------------------------------------------------------
#define EOS(ch) ((ch) == '\0' || ascii_isspace(ch))
bool SplitRange(const char* rangestr, int* from, int* to) {
    char* val = const_cast<char*>(rangestr);
    if (val == nullptr || EOS(*val)) return true;

    if (val[0] == '-' && EOS(val[1])) return true;

    if (val[0] == '-') {
        const int int2 = strto32(val + 1, &val, 10);
        if (!EOS(*val)) return false;
        *to = int2;
        return true;
    } else {
        const int int1 = strto32(val, &val, 10);
        if (EOS(*val) || (*val == '-' && EOS(*(val + 1)))) {
            *from = int1;
            return true;
        } else if (*val != '-') {
            return false;
        }
        const int int2 = strto32(val + 1, &val, 10);
        if (!EOS(*val)) return false;
        *from = int1;
        *to = int2;
        return true;
    }
}

void SplitCSVLineWithDelimiter(char* line, char delimiter, vector<char*>* cols) {
    char* end_of_line = line + strlen(line);
    char* end;
    char* start;

    for (; line < end_of_line; line++) {
        while (ascii_isspace(*line) && *line != delimiter) ++line;

        if (*line == '"' && delimiter == ',') {
            start = ++line;
            end = start;
            for (; *line; line++) {
                if (*line == '"') {
                    line++;
                    if (*line != '"') break;
                }
                *end++ = *line;
            }
            line = strchr(line, delimiter);
            if (!line) line = end_of_line;
        } else {
            start = line;
            line = strchr(line, delimiter);
            if (!line) line = end_of_line;
            for (end = line; end > start; --end) {
                if (!ascii_isspace(end[-1]) || end[-1] == delimiter) break;
            }
        }
        const bool need_another_column = (*line == delimiter) && (line == end_of_line - 1);
        *end = '\0';
        cols->push_back(start);
        if (need_another_column) cols->push_back(end);
        assert(*line == '\0' || *line == delimiter);
    }
}

void SplitCSVLine(char* line, vector<char*>* cols) {
    SplitCSVLineWithDelimiter(line, ',', cols);
}

void SplitCSVLineWithDelimiterForStrings(const string& line, char delimiter, vector<string>* cols) {
    char* cline = strndup_with_new(line.c_str(), line.size());
    vector<char*> v;
    SplitCSVLineWithDelimiter(cline, delimiter, &v);
    for (auto ci : v) {
        cols->push_back(ci);
    }
    delete[] cline;
}

// ----------------------------------------------------------------------
namespace {

class ClosingSymbolLookup {
public:
    explicit ClosingSymbolLookup(const char* symbol_pairs) {
        for (const char* symbol = symbol_pairs; *symbol != 0; ++symbol) {
            unsigned char opening = *symbol;
            ++symbol;
            unsigned char closing = *symbol != 0 ? *symbol : opening;
            closing_[opening] = closing;
            valid_closing_[closing] = true;
            if (*symbol == 0) break;
        }
    }

    char GetClosingChar(char opening) const { return closing_[static_cast<unsigned char>(opening)]; }
    bool IsClosing(char c) const { return valid_closing_[static_cast<unsigned char>(c)]; }

private:
    char closing_[256]{};
    bool valid_closing_[256]{};
    ClosingSymbolLookup(const ClosingSymbolLookup&) = delete;
    const ClosingSymbolLookup& operator=(const ClosingSymbolLookup&) = delete;
};

char* SplitStructuredLineInternal(char* line, char delimiter, const char* symbol_pairs, vector<char*>* cols,
                                  bool with_escapes) {
    ClosingSymbolLookup lookup(symbol_pairs);
    vector<char> expected_to_close;
    bool in_escape = false;
    CHECK(cols);
    cols->push_back(line);
    char* current;
    for (current = line; *current; ++current) {
        char c = *current;
        if (in_escape) {
            in_escape = false;
        } else if (with_escapes && c == '\\') {
            in_escape = true;
        } else if (expected_to_close.empty() && c == delimiter) {
            *current = 0;
            cols->push_back(current + 1);
        } else if (!expected_to_close.empty() && c == expected_to_close.back()) {
            expected_to_close.pop_back();
        } else if (lookup.GetClosingChar(c)) {
            expected_to_close.push_back(lookup.GetClosingChar(c));
        } else if (lookup.IsClosing(c)) {
            return current;
        }
    }
    if (!expected_to_close.empty()) {
        return current;
    }
    return nullptr;
}

bool SplitStructuredLineInternal(StringPiece line, char delimiter, const char* symbol_pairs, vector<StringPiece>* cols,
                                 bool with_escapes) {
    ClosingSymbolLookup lookup(symbol_pairs);
    vector<char> expected_to_close;
    bool in_escape = false;
    CHECK_NOTNULL(cols);
    cols->push_back(line);
    for (int i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_escape) {
            in_escape = false;
        } else if (with_escapes && c == '\\') {
            in_escape = true;
        } else if (expected_to_close.empty() && c == delimiter) {
            cols->back().remove_suffix(line.size() - i);
            cols->push_back(StringPiece(line, i + 1));
        } else if (!expected_to_close.empty() && c == expected_to_close.back()) {
            expected_to_close.pop_back();
        } else if (lookup.GetClosingChar(c)) {
            expected_to_close.push_back(lookup.GetClosingChar(c));
        } else if (lookup.IsClosing(c)) {
            return false;
        }
    }
    if (!expected_to_close.empty()) {
        return false;
    }
    return true;
}

} // anonymous namespace

char* SplitStructuredLine(char* line, char delimiter, const char* symbol_pairs, vector<char*>* cols) {
    return SplitStructuredLineInternal(line, delimiter, symbol_pairs, cols, false);
}

bool SplitStructuredLine(StringPiece line, char delimiter, const char* symbol_pairs, vector<StringPiece>* cols) {
    return SplitStructuredLineInternal(line, delimiter, symbol_pairs, cols, false);
}

char* SplitStructuredLineWithEscapes(char* line, char delimiter, const char* symbol_pairs, vector<char*>* cols) {
    return SplitStructuredLineInternal(line, delimiter, symbol_pairs, cols, true);
}

bool SplitStructuredLineWithEscapes(StringPiece line, char delimiter, const char* symbol_pairs,
                                    vector<StringPiece>* cols) {
    return SplitStructuredLineInternal(line, delimiter, symbol_pairs, cols, true);
}

// ----------------------------------------------------------------------
// SplitStringIntoKeyValues()
// ----------------------------------------------------------------------
bool SplitStringIntoKeyValues(const string& line, const string& key_value_delimiters,
                              const string& value_value_delimiters, string* key, vector<string>* values) {
    key->clear();
    values->clear();

    size_t end_key_pos = line.find_first_of(key_value_delimiters);
    if (end_key_pos == string::npos) {
        VLOG(2) << "cannot parse key from line: " << line;
        return false;
    }
    key->assign(line, 0, end_key_pos);

    string remains(line, end_key_pos, line.size() - end_key_pos);
    size_t begin_values_pos = remains.find_first_not_of(key_value_delimiters);
    if (begin_values_pos == string::npos) {
        VLOG(2) << "cannot parse value from line: " << line;
        return false;
    }
    string values_string(remains, begin_values_pos, remains.size() - begin_values_pos);

    if (value_value_delimiters.empty()) {
        values->push_back(values_string);
    } else {
        SplitStringUsing(values_string, value_value_delimiters.c_str(), values);
        if (values->size() < 1) {
            VLOG(2) << "cannot parse value from line: " << line;
            return false;
        }
    }
    return true;
}

bool SplitStringIntoKeyValuePairs(const string& line, const string& key_value_delimiters,
                                  const string& key_value_pair_delimiters, vector<pair<string, string> >* kv_pairs) {
    kv_pairs->clear();

    vector<string> pairs;
    SplitStringUsing(line, key_value_pair_delimiters.c_str(), &pairs);

    bool success = true;
    for (const auto& pair : pairs) {
        string key;
        vector<string> value;
        if (!SplitStringIntoKeyValues(pair, key_value_delimiters, "", &key, &value)) {
            success = false;
        }
        DCHECK_LE(value.size(), 1);
        kv_pairs->push_back(make_pair(key, value.empty() ? "" : value[0]));
    }
    return success;
}

// ----------------------------------------------------------------------
// SplitLeadingDec*Values()
// ----------------------------------------------------------------------
const char* SplitLeadingDec32Values(const char* str, vector<int32>* result) {
    for (;;) {
        char* end = nullptr;
        long value = strtol(str, &end, 10);
        if (end == str) break;
        if (value > numeric_limits<int32>::max()) {
            value = numeric_limits<int32>::max();
        } else if (value < numeric_limits<int32>::min()) {
            value = numeric_limits<int32>::min();
        }
        result->push_back(value);
        str = end;
        if (!ascii_isspace(*end)) break;
    }
    return str;
}

const char* SplitLeadingDec64Values(const char* str, vector<int64>* result) {
    for (;;) {
        char* end = nullptr;
        const int64 value = strtoll(str, &end, 10);
        if (end == str) break;
        result->push_back(value);
        str = end;
        if (!ascii_isspace(*end)) break;
    }
    return str;
}

void SplitStringToLines(const char* full, int max_len, int num_lines, vector<string>* result) {
    if (max_len <= 0) {
        return;
    }
    int pos = 0;
    for (int i = 0; (i < num_lines || num_lines <= 0); i++) {
        int cut_at = ClipStringHelper(full + pos, max_len, (i == num_lines - 1));
        if (cut_at == -1) {
            result->push_back(string(full + pos));
            return;
        }
        result->push_back(string(full + pos, cut_at));
        if (i == num_lines - 1 && max_len > kCutStrSize) {
            result->at(i).append(kCutStr);
        }
        pos += cut_at;
    }
}
