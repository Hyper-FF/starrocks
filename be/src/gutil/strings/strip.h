// Copyright 2011 Google Inc. All Rights Reserved.
// Refactored from contributions of various authors in strings/strutil.h
//
// This file contains functions that remove a defined part from the string,
// i.e., strip the string.
//
// Implementations delegate to absl where possible.
// New code should prefer absl::StripPrefix/StripSuffix/StripAsciiWhitespace.

#pragma once

#include <cstddef>
#include <string>
using std::string;

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/strip.h"
#include "gutil/strings/stringpiece.h"

// Given a string and a putative prefix, returns the string minus the
// prefix string if the prefix matches, otherwise the original string.
inline string StripPrefixString(StringPiece str, const StringPiece& prefix) {
    if (str.starts_with(prefix)) str.remove_prefix(prefix.length());
    return str.as_string();
}

// Like StripPrefixString, but return true if the prefix was
// successfully matched.  Write the output to *result.
inline bool TryStripPrefixString(StringPiece str, const StringPiece& prefix, string* result) {
    const bool has_prefix = str.starts_with(prefix);
    if (has_prefix) str.remove_prefix(prefix.length());
    *result = str.as_string();
    return has_prefix;
}

// Given a string and a putative suffix, returns the string minus the
// suffix string if the suffix matches, otherwise the original string.
inline string StripSuffixString(StringPiece str, const StringPiece& suffix) {
    if (str.ends_with(suffix)) str.remove_suffix(suffix.length());
    return str.as_string();
}

// Like StripSuffixString, but return true if the suffix was
// successfully matched.  Write the output to *result.
inline bool TryStripSuffixString(StringPiece str, const StringPiece& suffix, string* result) {
    const bool has_suffix = str.ends_with(suffix);
    if (has_suffix) str.remove_suffix(suffix.length());
    *result = str.as_string();
    return has_suffix;
}

// ----------------------------------------------------------------------
// StripString
//    Replaces any occurrence of the character 'remove' (or the characters
//    in 'remove') with the character 'replacewith'.
// ----------------------------------------------------------------------
inline void StripString(char* str, char remove, char replacewith) {
    for (; *str; str++) {
        if (*str == remove) *str = replacewith;
    }
}

void StripString(char* str, StringPiece remove, char replacewith);
void StripString(char* str, int len, StringPiece remove, char replacewith);
void StripString(string* s, StringPiece remove, char replacewith);

// ----------------------------------------------------------------------
// StripDupCharacters
//    Replaces any repeated occurrence of the character 'dup_char'
//    with single occurrence.
// ----------------------------------------------------------------------
int StripDupCharacters(string* s, char dup_char, int start_pos);

// ----------------------------------------------------------------------
// StripWhiteSpace
//    Removes whitespace from both sides of string.
// ----------------------------------------------------------------------
void StripWhiteSpace(const char** str, int* len);

//------------------------------------------------------------------------
// StripTrailingWhitespace()
//   Removes whitespace at the end of the string *s.
//------------------------------------------------------------------------
inline void StripTrailingWhitespace(string* s) {
    absl::string_view stripped = absl::StripTrailingAsciiWhitespace(*s);
    s->resize(stripped.size());
}

//------------------------------------------------------------------------
// StripTrailingNewline(string*)
//   Strips the very last trailing newline or CR+newline from its input.
//------------------------------------------------------------------------
bool StripTrailingNewline(string* s);

inline void StripWhiteSpace(char** str, int* len) {
    StripWhiteSpace(const_cast<const char**>(str), len);
}

inline void StripWhiteSpace(StringPiece* str) {
    const char* data = str->data();
    int len = str->size();
    StripWhiteSpace(&data, &len);
    str->set(data, len);
}

inline void StripWhiteSpace(string* str) {
    *str = std::string(absl::StripAsciiWhitespace(*str));
}

namespace strings {

template <typename Collection>
inline void StripWhiteSpaceInCollection(Collection* collection) {
    for (typename Collection::iterator it = collection->begin(); it != collection->end(); ++it) StripWhiteSpace(&(*it));
}

} // namespace strings

// ----------------------------------------------------------------------
// StripLeadingWhiteSpace
//    "Removes" whitespace from beginning of string.
// ----------------------------------------------------------------------

inline const char* StripLeadingWhiteSpace(const char* line) {
    while (absl::ascii_isspace(static_cast<unsigned char>(*line))) ++line;
    if ('\0' == *line) return nullptr;
    return line;
}

inline char* StripLeadingWhiteSpace(char* line) {
    return const_cast<char*>(StripLeadingWhiteSpace(const_cast<const char*>(line)));
}

inline void StripLeadingWhiteSpace(string* str) {
    absl::string_view stripped = absl::StripLeadingAsciiWhitespace(*str);
    if (stripped.size() != str->size()) {
        *str = std::string(stripped);
    }
}

// Remove leading, trailing, and duplicate internal whitespace.
void RemoveExtraWhitespace(string* s);

// ----------------------------------------------------------------------
// SkipLeadingWhiteSpace
//    Returns str advanced past white space characters, if any.
// ----------------------------------------------------------------------
inline const char* SkipLeadingWhiteSpace(const char* str) {
    while (absl::ascii_isspace(static_cast<unsigned char>(*str))) ++str;
    return str;
}

inline char* SkipLeadingWhiteSpace(char* str) {
    while (absl::ascii_isspace(static_cast<unsigned char>(*str))) ++str;
    return str;
}

// Misc. stripping routines
void StripCurlyBraces(string* s);
void StripBrackets(char left, char right, string* s);
void StripMarkupTags(string* s);
string OutputWithMarkupTagsStripped(const string& s);

int TrimStringLeft(string* s, const StringPiece& remove);
int TrimStringRight(string* s, const StringPiece& remove);

inline int TrimString(string* s, const StringPiece& remove) {
    return TrimStringRight(s, remove) + TrimStringLeft(s, remove);
}

void TrimRunsInString(string* s, StringPiece remove);
void RemoveNullsInString(string* s);

int strrm(char* str, char c);
int memrm(char* str, int strlen, char c);
int strrmm(char* str, const char* chars);
int strrmm(string* str, const string& chars);
