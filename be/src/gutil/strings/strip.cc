// Copyright 2011 Google Inc. All Rights Reserved.
// based on contributions of various authors in strings/strutil_unittest.cc
//
// This file contains functions that remove a defined part from the string,
// i.e., strip the string.
// Key functions delegate to absl (see strip.h inline definitions).

#include "gutil/strings/strip.h"

#include <algorithm>
#include <cassert>
#include <cstring>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <string>
using std::string;

#include "absl/strings/ascii.h"
#include "gutil/strings/stringpiece.h"

// ----------------------------------------------------------------------
// StripString
// ----------------------------------------------------------------------
void StripString(char* str, StringPiece remove, char replacewith) {
    for (; *str != '\0'; ++str) {
        if (remove.find(*str) != StringPiece::npos) {
            *str = replacewith;
        }
    }
}

void StripString(char* str, int len, StringPiece remove, char replacewith) {
    char* end = str + len;
    for (; str < end; ++str) {
        if (remove.find(*str) != StringPiece::npos) {
            *str = replacewith;
        }
    }
}

void StripString(string* s, StringPiece remove, char replacewith) {
    for (char& c : *s) {
        if (remove.find(c) != StringPiece::npos) {
            c = replacewith;
        }
    }
}

// ----------------------------------------------------------------------
// StripWhiteSpace (const char**, int*)
// ----------------------------------------------------------------------
void StripWhiteSpace(const char** str, int* len) {
    // strip off trailing whitespace
    while ((*len) > 0 && absl::ascii_isspace(static_cast<unsigned char>((*str)[(*len) - 1]))) {
        (*len)--;
    }
    // strip off leading whitespace
    while ((*len) > 0 && absl::ascii_isspace(static_cast<unsigned char>((*str)[0]))) {
        (*len)--;
        (*str)++;
    }
}

bool StripTrailingNewline(string* s) {
    if (!s->empty() && (*s)[s->size() - 1] == '\n') {
        if (s->size() > 1 && (*s)[s->size() - 2] == '\r')
            s->resize(s->size() - 2);
        else
            s->resize(s->size() - 1);
        return true;
    }
    return false;
}

// ----------------------------------------------------------------------
// Misc. stripping routines
// ----------------------------------------------------------------------
void StripCurlyBraces(string* s) {
    return StripBrackets('{', '}', s);
}

void StripBrackets(char left, char right, string* s) {
    string::iterator opencurly = find(s->begin(), s->end(), left);
    while (opencurly != s->end()) {
        string::iterator closecurly = find(opencurly, s->end(), right);
        if (closecurly == s->end()) return;
        opencurly = s->erase(opencurly, closecurly + 1);
        opencurly = find(opencurly, s->end(), left);
    }
}

void StripMarkupTags(string* s) {
    string::iterator openbracket = find(s->begin(), s->end(), '<');
    while (openbracket != s->end()) {
        string::iterator closebracket = find(openbracket, s->end(), '>');
        if (closebracket == s->end()) {
            s->erase(openbracket, closebracket);
            return;
        }
        openbracket = s->erase(openbracket, closebracket + 1);
        openbracket = find(openbracket, s->end(), '<');
    }
}

string OutputWithMarkupTagsStripped(const string& s) {
    string result(s);
    StripMarkupTags(&result);
    return result;
}

int TrimStringLeft(string* s, const StringPiece& remove) {
    int i = 0;
    while (i < s->size() && memchr(remove.data(), (*s)[i], remove.size())) {
        ++i;
    }
    if (i > 0) s->erase(0, i);
    return i;
}

int TrimStringRight(string* s, const StringPiece& remove) {
    int i = s->size(), trimmed = 0;
    while (i > 0 && memchr(remove.data(), (*s)[i - 1], remove.size())) {
        --i;
    }
    if (i < s->size()) {
        trimmed = s->size() - i;
        s->erase(i);
    }
    return trimmed;
}

// ----------------------------------------------------------------------
// Various removal routines
// ----------------------------------------------------------------------
int strrm(char* str, char c) {
    char *src, *dest;
    for (src = dest = str; *src != '\0'; ++src)
        if (*src != c) *(dest++) = *src;
    *dest = '\0';
    return dest - str;
}

int memrm(char* str, int strlen, char c) {
    char *src, *dest;
    for (src = dest = str; strlen-- > 0; ++src)
        if (*src != c) *(dest++) = *src;
    return dest - str;
}

int strrmm(char* str, const char* chars) {
    char *src, *dest;
    for (src = dest = str; *src != '\0'; ++src) {
        bool skip = false;
        for (const char* c = chars; *c != '\0'; c++) {
            if (*src == *c) {
                skip = true;
                break;
            }
        }
        if (!skip) *(dest++) = *src;
    }
    *dest = '\0';
    return dest - str;
}

int strrmm(string* str, const string& chars) {
    size_t str_len = str->length();
    size_t in_index = str->find_first_of(chars);
    if (in_index == string::npos) return str_len;

    size_t out_index = in_index++;

    while (in_index < str_len) {
        char c = (*str)[in_index++];
        if (chars.find(c) == string::npos) (*str)[out_index++] = c;
    }

    str->resize(out_index);
    return out_index;
}

int StripDupCharacters(string* s, char dup_char, int start_pos) {
    if (start_pos < 0) start_pos = 0;

    int input_pos = start_pos;
    int output_pos = start_pos;
    const int input_end = s->size();
    while (input_pos < input_end) {
        const char curr_char = (*s)[input_pos];
        if (output_pos != input_pos) (*s)[output_pos] = curr_char;
        ++input_pos;
        ++output_pos;

        if (curr_char == dup_char) {
            while ((input_pos < input_end) && ((*s)[input_pos] == dup_char)) ++input_pos;
        }
    }
    const int num_deleted = input_pos - output_pos;
    s->resize(s->size() - num_deleted);
    return num_deleted;
}

void RemoveExtraWhitespace(string* s) {
    assert(s != nullptr);
    if (s->empty()) return;

    int input_pos = 0;
    int output_pos = 0;
    const int input_end = s->size();
    // Strip off leading space
    while (input_pos < input_end && absl::ascii_isspace(static_cast<unsigned char>((*s)[input_pos]))) input_pos++;

    while (input_pos < input_end - 1) {
        char c = (*s)[input_pos];
        char next = (*s)[input_pos + 1];
        if (!absl::ascii_isspace(static_cast<unsigned char>(c)) ||
            !absl::ascii_isspace(static_cast<unsigned char>(next))) {
            if (output_pos != input_pos) {
                (*s)[output_pos] = c;
            }
            output_pos++;
        }
        input_pos++;
    }
    char c = (*s)[input_end - 1];
    if (!absl::ascii_isspace(static_cast<unsigned char>(c))) (*s)[output_pos++] = c;

    s->resize(output_pos);
}

void TrimRunsInString(string* s, StringPiece remove) {
    string::iterator dest = s->begin();
    string::iterator src_end = s->end();
    for (string::iterator src = s->begin(); src != src_end;) {
        if (remove.find(*src) == StringPiece::npos) {
            *(dest++) = *(src++);
        } else {
            for (++src; src != src_end; ++src) {
                if (remove.find(*src) == StringPiece::npos) {
                    if (dest != s->begin()) {
                        *(dest++) = remove[0];
                    }
                    *(dest++) = *(src++);
                    break;
                }
            }
        }
    }
    s->erase(dest, src_end);
}

void RemoveNullsInString(string* s) {
    s->erase(remove(s->begin(), s->end(), '\0'), s->end());
}
