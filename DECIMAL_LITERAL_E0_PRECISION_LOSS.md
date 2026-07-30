# Deparser 对整数值 DECIMAL 字面量补 `E0` 导致往返丢精度

- Status: 已复现，未修复
- Date: 2026-07-29
- Baseline: main @ `8c2647a7b6d`
- 发现途径: `SQL_AST_FUZZER_PLAN.md` P0 round-trip 检查（修完 INSERT VALUES bug 后暴露）
- 相关: `DEPARSER_ROUNDTRIP_P0_REPORT.md` §3.6

## 现象

一条 SQL 经过 `parse → AstToSQLBuilder.toSQL → parse` 之后，超大整数字面量的**值被静默改变**：

```
in:      select 1234567890123456789012345678901234567890123456789012345678901234567890123456
round 1: SELECT 1234567890123456789012345678901234567890123456789012345678901234567890123456E0
round 2: SELECT 1.2345678901234569E75
```

76 位有效数字塌缩为 17 位。往返不是不动点，且**不报错、不告警**。

## 根因

`fe/fe-core/src/main/java/com/starrocks/sql/formatter/AST2StringVisitor.java:1386-1392`

```java
@Override
public String visitLiteral(LiteralExpr node, Void context) {
    if (node instanceof DecimalLiteral) {
        if ((((DecimalLiteral) node).getValue().scale() == 0)) {
            return ((DecimalLiteral) node).getValue().toString() + "E0";   // <-- 这里
        } else {
            return visitExpression(node, context);
        }
    }
    ...
```

对 `scale() == 0`（即整数值）的 `DecimalLiteral`，deparser 追加 `E0` 后缀。

**这个后缀的意图应该是保类型**：不加后缀的话 `123` 重新解析会变成整数字面量（TINYINT/INT/BIGINT），
DECIMAL 类型信息在往返中丢失。加 `E0` 强制解析器按非整数字面量处理。

**代价是丢值**：`<digits>E0` 是科学计数法，解析器按 DOUBLE 处理，
而 DOUBLE 只有 53 位尾数 ≈ 15–17 位十进制有效数字。超出部分静默截断。

即：这是一处**用值保真换类型保真**的取舍，但取舍本身没有被记录，且没有对超限值做保护。

## 影响面（已用实测收敛，比初判窄得多）

命中条件由 parser 侧的字面量选型决定
（`fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java:7938-7947`）：

```java
if (intLiteral.compareTo(LONG_MAX) <= 0) {
    return new IntLiteral(intLiteral.longValue(), pos);
} else if (intLiteral.compareTo(LARGEINT_MAX_ABS) <= 0) {
    return new LargeIntLiteral(intLiteral.toString(), pos);
} else if (intLiteral.compareTo(INT256_MAX_ABS) <= 0) {
    return new DecimalLiteral(intLiteral.toString(), pos);   // <-- 只有这一档会被 E0 破坏
} else {
    throw new ParsingException(PARSER_ERROR_MSG.numOverflow(intText), pos);
}
```

所以**受影响的区间恰好是 `(LARGEINT_MAX_ABS, INT256_MAX_ABS]`**，
即"大到 LARGEINT 装不下、必须用 DECIMAL256/INT256 表示的整数字面量"。

- ≤ LONG_MAX → `IntLiteral`，不受影响
- ≤ 2^127 → `LargeIntLiteral`，不受影响（DECIMAL(38,0) 全部落在这里，**安全**）
- 带小数位的字面量不受影响（`scale()!=0` 走 `visitExpression`），实测 50 位小数仍稳定

### 阈值实测

| 字面量 | 往返 1 轮 | 往返 2 轮 | 稳定? |
|---|---|---|:--:|
| `100` | `100` | `100` | ✅ |
| `100.5` | `100.5` | `100.5` | ✅ |
| `12345678901234567`（17 位） | 原样 | 原样 | ✅ |
| `1234567890123456789`（19 位） | 原样 | 原样 | ✅ |
| `123456789012345678901234567890`（30 位） | 原样 | 原样 | ✅ |
| `12345678901234567890123456789012345678`（38 位，DECIMAL128 上限） | 原样 | 原样 | ✅ |
| `170141183460469231731687303715884105727`（2^127−1） | 原样 | 原样 | ✅ |
| `170141183460469231731687303715884105728`（2^127 = LARGEINT_MAX_ABS） | 原样 | 原样 | ✅ |
| `999999999999999999999999999999999999999`（39 个 9） | `…9E0` | **`1.0E39`** | ❌ |
| `1234567890123456789012345678901234567890`（40 位） | `…0E0` | **`1.2345678901234568E39`** | ❌ |
| `100.1234…`（50 位小数） | 原样 | 原样 | ✅ |
| 原始发现用例（76 位） | `…6E0` | **`1.2345678901234569E75`** | ❌ |

**结论**：这是 **DECIMAL256 / INT256 专属**的缺陷，DECIMAL128 及以下完全不受影响。
严重度因此从"普遍"降为"新特性局部"，但在该特性范围内仍是**静默改值**。

语料中的命中来源正是 `test/sql` 的 `decimal256_agg_test` / `decimal_window_test` 等用例。

## 生产可达性

**尚未评估**。需要确认的路径：视图 / MV 的 `inlineViewDef` 以 deparse 后的文本存储
（`ViewAnalyzer:94`、`MaterializedViewAnalyzer:363`），若定义中含此类字面量，
存储的定义与用户所写不再等价，后续从存储文本重建计划时用的是被改变的值。

## 复现

```bash
cd fe && JAVA_HOME=/home/public/jdk-17.0.2 \
  mvn -Pfuzz -pl fe-fuzz -am -DskipTests -Dcheckstyle.skip=true compile

java -Dlog4j2.disable.jmx=true -cp "fe/fe-fuzz/target/classes:<fe-core-classpath>" \
  com.starrocks.fuzz.DeparseIdempotenceProbe 3 \
  "select 1234567890123456789012345678901234567890123456789012345678901234567890123456"
```

⚠️ 若此前跑过 `mvn test`，`fe-core/target/classes` 会被 JaCoCo 离线插桩，
直接 `java -cp` 会报 `NoClassDefFoundError: org/jacoco/agent/rt/...`。
需先 `rm -rf fe/fe-core/target/classes` 再用 `-Djacoco.skip=true` 重新编译。

## 修复方向（未实施，按代价排序）

1. **超限时退回带引号或带 CAST 的形式**：当 `precision > 17` 时，
   输出 `CAST('<digits>' AS DECIMAL(p,0))` 或直接输出裸数字并接受类型退化，
   而不是产出一个必然丢值的 `E0` 形式。丢类型比丢值好。
2. **改用不丢值的类型保持写法**：确认 parser 是否接受 `DECIMAL` 字面量后缀或
   `<digits>.0` 形式（`scale=1` 会走 `visitExpression` 分支）——若 `1234...3456.` 能被解析成
   DECIMAL 而非 DOUBLE，改成补小数点比补 `E0` 更安全。**需先验证 parser 行为。**
3. 无论选哪种，都应补一条断言"deparse 后再解析，字面量的值必须不变"的 UT，
   并纳入 `DeparseIdempotenceProbe` 的对照组。

## 待办

- [x] 填齐阈值实测表 → 受影响区间收敛为 `(LARGEINT_MAX_ABS, INT256_MAX_ABS]`
- [ ] 验证方向 2：parser 对 `<digits>.` / `<digits>.0` 的类型判定
- [ ] 评估生产可达性：构造含 DECIMAL256 字面量的 view / MV，检查 `SHOW CREATE VIEW` 与实际查询结果
- [ ] 决定修复方案并提 PR

## 修复时的注意点

`visitLiteral` 里 `E0` 分支对**所有** `scale()==0` 的 `DecimalLiteral` 生效，
但只有超出 LARGEINT 的那一档会由 parser 产生 DecimalLiteral——
换言之当前代码路径里 `E0` 几乎只服务于 DECIMAL256。
修的时候要确认：是否还有别的途径构造出小值的 `DecimalLiteral`（例如 CAST 折叠、常量传播、
`assignValues` 参数替换），否则改动可能影响到未被这次测量覆盖的路径。
