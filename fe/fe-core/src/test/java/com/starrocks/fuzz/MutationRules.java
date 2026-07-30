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

package com.starrocks.fuzz;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Declarative rule set deciding where the mutator may act.
 *
 * <p>Mutating through {@code TreeNode.setChild} can build trees the parser could never produce -- an
 * {@code ExistsPredicate} whose child is a {@code SlotRef}, a lambda whose body is another lambda. Any
 * finding from such a tree is an artifact of the mutator, not a defect in StarRocks. Rather than hard-code
 * the exclusions, they are declared in XML so a new one costs a rule, not a recompile.
 *
 * <p>Loaded from {@code -Dsrfuzz.rules=<path>}, else the bundled {@code fuzz/mutation-rules.xml}.
 * {@code -Dsrfuzz.gate=off} ignores the rules entirely, which is how their effect gets measured.
 *
 * <pre>{@code
 * <fuzz-rules>
 *   <!-- A rule fires when ANY of its <match> elements fires.
 *        A <match> fires when ALL of the attributes it specifies hold.
 *        action="allow" wins over action="skip", so a broad skip can carve out exceptions. -->
 *   <rule name="exists-takes-only-a-subquery" action="skip">
 *     <match parent-type="ExistsPredicate"/>
 *   </rule>
 *   <rule name="time-slice-boundary-is-a-keyword" action="skip">
 *     <match parent-function="time_slice" index="2"/>
 *   </rule>
 * </fuzz-rules>
 * }</pre>
 *
 * <p>Recognised {@code <match>} attributes, all optional:
 * <ul>
 *   <li>{@code parent-type} / {@code child-type} — simple class name of the node</li>
 *   <li>{@code parent-function} / {@code child-function} — function name, when that node is a call</li>
 *   <li>{@code index} — position of the child under its parent</li>
 * </ul>
 */
public final class MutationRules {

    public static final String DEFAULT_RESOURCE = "fuzz/mutation-rules.xml";

    private static volatile MutationRules instance;

    private final boolean enabled;
    private final List<Rule> rules;
    private final String origin;

    private MutationRules(boolean enabled, List<Rule> rules, String origin) {
        this.enabled = enabled;
        this.rules = rules;
        this.origin = origin;
    }

    // ------------------------------------------------------------------ model

    private enum Action { SKIP, ALLOW }

    private static final class Match {
        String parentType;
        String childType;
        String parentFunction;
        String childFunction;
        Integer index;

        boolean matches(Expr parent, int idx, Expr child) {
            if (index != null && index != idx) {
                return false;
            }
            if (parentType != null && !parentType.equals(parent.getClass().getSimpleName())) {
                return false;
            }
            if (childType != null && !childType.equals(child.getClass().getSimpleName())) {
                return false;
            }
            if (parentFunction != null && !parentFunction.equals(functionName(parent))) {
                return false;
            }
            return childFunction == null || childFunction.equals(functionName(child));
        }

        /** An empty match would fire on every position; treat that as a configuration error. */
        boolean isEmpty() {
            return parentType == null && childType == null && parentFunction == null
                    && childFunction == null && index == null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{");
            if (parentType != null) {
                sb.append("parent-type=").append(parentType).append(' ');
            }
            if (childType != null) {
                sb.append("child-type=").append(childType).append(' ');
            }
            if (parentFunction != null) {
                sb.append("parent-function=").append(parentFunction).append(' ');
            }
            if (childFunction != null) {
                sb.append("child-function=").append(childFunction).append(' ');
            }
            if (index != null) {
                sb.append("index=").append(index).append(' ');
            }
            return sb.toString().trim() + "}";
        }
    }

    static final class Rule {
        String name;
        Action action = Action.SKIP;
        final List<Match> matches = new ArrayList<>();

        boolean fires(Expr parent, int idx, Expr child) {
            for (Match m : matches) {
                if (m.matches(parent, idx, child)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static String functionName(Expr e) {
        if (!(e instanceof FunctionCallExpr)) {
            return null;
        }
        FunctionCallExpr call = (FunctionCallExpr) e;
        if (call.getFnName() == null) {
            return null;
        }
        List<String> parts = call.getFnName().getParts();
        return parts.isEmpty() ? null : parts.get(parts.size() - 1).toLowerCase();
    }

    // ----------------------------------------------------------------- lookup

    /** True when the mutator must leave child {@code index} of {@code parent} alone. */
    public boolean isBlocked(Expr parent, int index) {
        if (!enabled) {
            return false;
        }
        Expr child = parent.getChild(index);
        boolean blocked = false;
        for (Rule rule : rules) {
            if (!rule.fires(parent, index, child)) {
                continue;
            }
            if (rule.action == Action.ALLOW) {
                return false;
            }
            blocked = true;
        }
        return blocked;
    }

    /** Builds a rule set directly, for tests that pin the matching semantics down. */
    static MutationRules forTesting(List<Rule> rules) {
        return new MutationRules(true, rules, "<test>");
    }

    public String describe() {
        return "srfuzz rules: " + (enabled ? rules.size() + " rule(s) from " + origin : "DISABLED");
    }

    // ------------------------------------------------------------------ load

    public static MutationRules get() {
        MutationRules local = instance;
        if (local == null) {
            synchronized (MutationRules.class) {
                local = instance;
                if (local == null) {
                    local = load();
                    instance = local;
                    System.err.println(local.describe());
                }
            }
        }
        return local;
    }

    static MutationRules load() {
        if ("off".equalsIgnoreCase(System.getProperty("srfuzz.gate", "on"))) {
            return new MutationRules(false, List.of(), "<disabled>");
        }
        String path = System.getProperty("srfuzz.rules");
        try {
            if (path != null && !path.trim().isEmpty()) {
                Path file = Paths.get(path.trim());
                try (InputStream in = Files.newInputStream(file)) {
                    return new MutationRules(true, parse(in), file.toAbsolutePath().toString());
                }
            }
            try (InputStream in = MutationRules.class.getClassLoader().getResourceAsStream(DEFAULT_RESOURCE)) {
                if (in == null) {
                    System.err.println("srfuzz: " + DEFAULT_RESOURCE + " not on the classpath; no rules active");
                    return new MutationRules(true, List.of(), "<none>");
                }
                return new MutationRules(true, parse(in), DEFAULT_RESOURCE);
            }
        } catch (Exception e) {
            // A broken rule file must not silently turn the gate off -- that would quietly refill the
            // report with artifacts and look like a regression in StarRocks.
            throw new IllegalStateException("srfuzz: cannot load mutation rules from "
                    + (path != null ? path : DEFAULT_RESOURCE), e);
        }
    }

    static List<Rule> parse(InputStream in) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setExpandEntityReferences(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(in);

        List<Rule> parsed = new ArrayList<>();
        NodeList ruleNodes = doc.getElementsByTagName("rule");
        for (int i = 0; i < ruleNodes.getLength(); i++) {
            Element ruleEl = (Element) ruleNodes.item(i);
            Rule rule = new Rule();
            rule.name = attr(ruleEl, "name");
            String action = attr(ruleEl, "action");
            if (action != null && !action.isEmpty()) {
                rule.action = Action.valueOf(action.trim().toUpperCase());
            }
            NodeList children = ruleEl.getChildNodes();
            for (int j = 0; j < children.getLength(); j++) {
                Node node = children.item(j);
                if (node.getNodeType() != Node.ELEMENT_NODE || !"match".equals(node.getNodeName())) {
                    continue;
                }
                Element matchEl = (Element) node;
                Match match = new Match();
                match.parentType = attr(matchEl, "parent-type");
                match.childType = attr(matchEl, "child-type");
                match.parentFunction = lower(attr(matchEl, "parent-function"));
                match.childFunction = lower(attr(matchEl, "child-function"));
                String idx = attr(matchEl, "index");
                if (idx != null) {
                    match.index = Integer.parseInt(idx.trim());
                }
                if (match.isEmpty()) {
                    throw new IllegalStateException("srfuzz: rule '" + rule.name
                            + "' has a <match> with no attributes, which would block every position");
                }
                rule.matches.add(match);
            }
            if (rule.matches.isEmpty()) {
                throw new IllegalStateException("srfuzz: rule '" + rule.name + "' has no <match> element");
            }
            parsed.add(rule);
        }
        return parsed;
    }

    private static String attr(Element el, String name) {
        String v = el.getAttribute(name);
        return v == null || v.isEmpty() ? null : v;
    }

    private static String lower(String v) {
        return v == null ? null : v.toLowerCase();
    }
}
