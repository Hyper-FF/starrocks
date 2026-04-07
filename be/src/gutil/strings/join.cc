// Copyright 2008 and onwards Google Inc.  All rights reserved.
//
// Implementation migrated to absl::StrJoin (see join.h).
// This file retains only functions that have no absl equivalent.

#include "gutil/strings/join.h"

#include "gutil/gscoped_ptr.h"
#include "gutil/logging.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/escaping.h"

// ----------------------------------------------------------------------
// JoinUsing()
// ----------------------------------------------------------------------
char* JoinUsing(const vector<const char*>& components, const char* delim, int* result_length_p) {
    const int num_components = components.size();
    const int delim_length = strlen(delim);
    int num_chars = (num_components > 1) ? delim_length * (num_components - 1) : 0;
    for (int i = 0; i < num_components; ++i) num_chars += strlen(components[i]);

    auto res_buffer = new char[num_chars + 1];
    return JoinUsingToBuffer(components, delim, num_chars + 1, res_buffer, result_length_p);
}

char* JoinUsingToBuffer(const vector<const char*>& components, const char* delim, int result_buffer_size,
                        char* result_buffer, int* result_length_p) {
    CHECK(result_buffer != nullptr);
    const int num_components = components.size();
    const int max_str_len = result_buffer_size - 1;
    char* curr_dest = result_buffer;
    int num_chars = 0;
    for (int i = 0; (i < num_components) && (num_chars < max_str_len); ++i) {
        const char* curr_src = components[i];
        while ((*curr_src != '\0') && (num_chars < max_str_len)) {
            *curr_dest = *curr_src;
            ++num_chars;
            ++curr_dest;
            ++curr_src;
        }
        if (i != (num_components - 1)) {
            curr_src = delim;
            while ((*curr_src != '\0') && (num_chars < max_str_len)) {
                *curr_dest = *curr_src;
                ++num_chars;
                ++curr_dest;
                ++curr_src;
            }
        }
    }

    if (result_buffer_size > 0) *curr_dest = '\0';
    if (result_length_p != nullptr) *result_length_p = num_chars;

    return result_buffer;
}

// ----------------------------------------------------------------------
// JoinStringsInArray()
// ----------------------------------------------------------------------
void JoinStringsInArray(string const* const* components, int num_components, const char* delim, string* result) {
    CHECK(result != nullptr);
    result->clear();
    for (int i = 0; i < num_components; i++) {
        if (i > 0) {
            (*result) += delim;
        }
        (*result) += *(components[i]);
    }
}

void JoinStringsInArray(string const* components, int num_components, const char* delim, string* result) {
    JoinStringsIterator(components, components + num_components, delim, result);
}

// ----------------------------------------------------------------------
// JoinMapKeysAndValues() / JoinVectorKeysAndValues()
// ----------------------------------------------------------------------
void JoinMapKeysAndValues(const map<string, string>& components, const StringPiece& intra_delim,
                          const StringPiece& inter_delim, string* result) {
    JoinKeysAndValuesIterator(components.begin(), components.end(), intra_delim, inter_delim, result);
}

void JoinVectorKeysAndValues(const vector<pair<string, string> >& components, const StringPiece& intra_delim,
                             const StringPiece& inter_delim, string* result) {
    JoinKeysAndValuesIterator(components.begin(), components.end(), intra_delim, inter_delim, result);
}

// ----------------------------------------------------------------------
// JoinCSVLine()
// ----------------------------------------------------------------------
void JoinCSVLineWithDelimiter(const vector<string>& cols, char delimiter, string* output) {
    CHECK(output);
    CHECK(output->empty());
    vector<string> quoted_cols;

    const string delimiter_str(1, delimiter);
    const string escape_chars = delimiter_str + "\"";

    for (const auto& col : cols) {
        if ((col.find_first_of(escape_chars) != string::npos) ||
            (!col.empty() && (ascii_isspace(*col.begin()) || ascii_isspace(*col.rbegin())))) {
            int size = 2 * col.size() + 3;
            gscoped_array<char> buf(new char[size]);

            int escaped_size = strings::EscapeStrForCSV(col.c_str(), buf.get() + 1, size - 2);
            CHECK_GE(escaped_size, 0) << "Buffer somehow wasn't large enough.";
            CHECK_GE(size, escaped_size + 3) << "Buffer should have one space at the beginning for a "
                                             << "double-quote, one at the end for a double-quote, and "
                                             << "one at the end for a closing '\\0'";
            *buf.get() = '"';
            *((buf.get() + 1) + escaped_size) = '"';
            *((buf.get() + 1) + escaped_size + 1) = '\0';
            quoted_cols.emplace_back(buf.get(), buf.get() + escaped_size + 2);
        } else {
            quoted_cols.push_back(col);
        }
    }
    JoinStrings(quoted_cols, delimiter_str, output);
}

void JoinCSVLine(const vector<string>& cols, string* output) {
    JoinCSVLineWithDelimiter(cols, ',', output);
}

string JoinCSVLine(const vector<string>& cols) {
    string output;
    JoinCSVLine(cols, &output);
    return output;
}
