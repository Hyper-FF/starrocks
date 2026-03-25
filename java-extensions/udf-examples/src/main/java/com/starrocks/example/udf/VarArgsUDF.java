// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.example.udf;

/**
 * Example UDF demonstrating varargs support in StarRocks Java UDFs.
 *
 * <p>In StarRocks, a Java UDF can be declared with variable-length argument support
 * using the {@code ...} syntax in the CREATE FUNCTION statement:
 * <pre>
 *   CREATE FUNCTION my_concat(string, string, ...)
 *   RETURNS string
 *   PROPERTIES (
 *     "symbol" = "com.starrocks.example.udf.VarArgsUDF",
 *     "type" = "StarrocksJar",
 *     "file" = "path/to/udf.jar"
 *   );
 * </pre>
 *
 * <p>The Java method signature corresponds to the fixed declared argument types.
 * When the function is called with additional arguments matching the varargs type,
 * the extra arguments are passed through as additional column inputs.
 *
 * <p>SQL declaration example (two fixed string args with extra string varargs):
 * <pre>
 *   CREATE FUNCTION my_concat(string, string, ...) RETURNS string ...
 * </pre>
 * The Java method {@code evaluate(String a, String b)} handles the first two arguments.
 */
public class VarArgsUDF {

    /**
     * Concatenates two string arguments.
     * When declared as a varargs UDF {@code concat_str(string, string, ...)},
     * extra arguments beyond the first two are accepted at the SQL level.
     *
     * @param a first string argument
     * @param b second string argument
     * @return concatenation of a and b, or null if either is null
     */
    public String evaluate(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }
}
