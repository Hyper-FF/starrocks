#!/usr/bin/env bash
# srfuzz-launch.sh -- point the cluster fuzzer at a cluster and start it.
#
# The harness itself (clusterfuzz.next.sh) hard-codes one deployment: a fixed workspace path and
# `mysql -h127.0.0.1 -P9030 -uroot`. That is fine for the box it grew up on and useless anywhere
# else. This wraps it: you describe a cluster, it materialises a RUN DIRECTORY containing a copy of
# the harness with those two facts substituted, records everything it decided in run.conf, and
# starts N instances plus a watchdog.
#
# Why a copy rather than editing the harness: the harness is owned by another workstream, and a run
# whose parameters live in a file somebody else may rewrite is a run whose parameters you cannot
# trust afterwards. The copy makes each run self-describing and reproducible.
#
# The rule this file exists to enforce: AN ORACLE THAT CANNOT RUN MUST SAY SO. Every capability is
# probed up front -- can we reach the FE, is the BE registered, can we read be.out, may we restart a
# backend -- and whatever is missing is disabled EXPLICITLY and written into run.conf. A campaign
# that silently loses its crash oracle looks exactly like a campaign that found no crashes.
set -u

PROG=${0##*/}

usage() {
    cat <<'EOF'
用法:
  srfuzz-launch.sh start  [选项]      物化 run 目录并启动实例 + watchdog
  srfuzz-launch.sh status [选项]      看这次运行在不在推进（判据是轮次计数，不是进程）
  srfuzz-launch.sh stop   [选项]      停掉这次运行的实例和 watchdog

集群（必填）:
  --fe-host HOST            FE 地址
  --fe-port PORT            FE 查询端口（默认 9030）
  --fe-user USER            用户（默认 root）
  --fe-pass PASS            密码（默认空）
  --be-http HOST:PORT       BE 的 http 端口，用于读 /varz（默认 <fe-host>:8040）

本地能力（给什么就启用什么，缺什么就关掉什么并写进 run.conf）:
  --sr-home DIR             StarRocks 部署目录，含 output/{fe,be} 或 {fe,be}。
                            给了就启用：崩溃日志扫描、FE 日志签名、FE/BE 自动重启。
  --be-log FILE             直接指定 be.out（覆盖 --sr-home 的推导）
  --fe-log-dir DIR          直接指定 FE 日志目录
  --no-restart              即使能推导出重启脚本也不重启（对着别人的集群跑时用）

运行参数:
  --corpus DIR              语料目录（含 *.setup.sql / *.query.sql），必填
  --run-dir DIR             运行目录，默认 ./srfuzz-run-<时间戳>
  --harness FILE            clusterfuzz.next.sh 的路径，必填
  --instances N             并发实例数（默认 2）
  --seed N                  数据生成种子（默认当天日期，如 20260804）
  --diff-max-stmts N        每轮差分检查的语句数上限（默认 8）
  --no-knob-split           不按实例劈分 knob（默认劈分：每实例只跑一半，机队合起来仍覆盖全部）
  --container NAME          所有命令通过 `docker exec` 在该容器里执行
  --dry-run                 只做预检和物化，不启动实例（用来验证配置，不会碰正在跑的运行）

示例:
  srfuzz-launch.sh start --fe-host 127.0.0.1 --sr-home /home/disk1/fha/sr-ws/fuzz \
      --corpus /home/disk1/fha/sr-ws/fuzz/clusterfuzz/emit \
      --harness /home/disk1/fha/sr-ws/fuzz/clusterfuzz/clusterfuzz.next.sh \
      --run-dir /home/disk1/fha/sr-ws/fuzz/clusterfuzz/run-A --instances 2 --container sr-dev-fuzz
EOF
}

CMD=${1:-}; shift 2>/dev/null || true
case "$CMD" in start|status|stop) ;; *) usage; exit 1 ;; esac

FE_HOST=""; FE_PORT=9030; FE_USER=root; FE_PASS=""; BE_HTTP=""
SR_HOME=""; BE_LOG=""; FE_LOG_DIR=""; NO_RESTART=0
CORPUS=""; RUN_DIR=""; HARNESS=""; INSTANCES=2; SEED=""; DIFF_MAX_STMTS=8
KNOB_SPLIT=1; CONTAINER=""; DRY_RUN=0

while [ $# -gt 0 ]; do
    case "$1" in
        --fe-host) FE_HOST=$2; shift 2 ;;
        --fe-port) FE_PORT=$2; shift 2 ;;
        --fe-user) FE_USER=$2; shift 2 ;;
        --fe-pass) FE_PASS=$2; shift 2 ;;
        --be-http) BE_HTTP=$2; shift 2 ;;
        --sr-home) SR_HOME=$2; shift 2 ;;
        --be-log) BE_LOG=$2; shift 2 ;;
        --fe-log-dir) FE_LOG_DIR=$2; shift 2 ;;
        --no-restart) NO_RESTART=1; shift ;;
        --corpus) CORPUS=$2; shift 2 ;;
        --run-dir) RUN_DIR=$2; shift 2 ;;
        --harness) HARNESS=$2; shift 2 ;;
        --instances) INSTANCES=$2; shift 2 ;;
        --seed) SEED=$2; shift 2 ;;
        --diff-max-stmts) DIFF_MAX_STMTS=$2; shift 2 ;;
        --no-knob-split) KNOB_SPLIT=0; shift ;;
        --container) CONTAINER=$2; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "未知参数: $1" >&2; usage; exit 1 ;;
    esac
done

# Run a command where the cluster lives. Everything downstream goes through this, so a containerised
# deployment and a bare host differ in exactly one place.
sh_run() { if [ -n "$CONTAINER" ]; then docker exec -i "$CONTAINER" bash -lc "$1"; else bash -lc "$1"; fi; }
sh_run_d() { if [ -n "$CONTAINER" ]; then docker exec -d "$CONTAINER" bash -lc "$1"; else setsid nohup bash -lc "$1" >/dev/null 2>&1 & fi; }
have() { sh_run "test -e '$1'" >/dev/null 2>&1; }

die() { echo "错误: $*" >&2; exit 1; }
ok()  { echo "  [ok]   $*"; }
warn(){ echo "  [关闭] $*"; }

[ -n "$RUN_DIR" ] || RUN_DIR="$PWD/srfuzz-run-$(date +%Y%m%d-%H%M%S)"
CONF="$RUN_DIR/run.conf"

# ---------------------------------------------------------------- status / stop
if [ "$CMD" = status ] || [ "$CMD" = stop ]; then
    have "$CONF" || die "$CONF 不存在，--run-dir 指对了吗"
    # run.conf is written to be read by a human: values carry spaces and parentheses.
    # eval choked on them and left every RUN_* variable unset, so status reported
    # nonsense instead of failing. Parse it as data, never as shell.
    while IFS= read -r _line; do
        case "$_line" in
            [A-Z_]*=*) printf -v "${_line%%=*}" '%s' "${_line#*=}" ;;
        esac
    done <<EOF
$(sh_run "grep -E '^[A-Z_]+=' '$CONF'")
EOF
    if [ "$CMD" = stop ]; then
        sh_run "pkill -f '[s]rfuzz-watch-$RUN_ID'; pkill -f 'INSTANCE=.*$RUN_ID'; sleep 3; true"
        sh_run "pkill -f '$RUN_DIR/clusterfuzz.run.sh'; sleep 2; true"
        echo "已停止 $RUN_ID"
        exit 0
    fi
    echo "运行 $RUN_ID  (seed=$RUN_SEED, instances=$RUN_INSTANCES)"
    sh_run "now=\$(date +%s); for k in \$(seq 0 \$(( $RUN_INSTANCES - 1 ))); do
        f=$RUN_DIR/inst\$k/rounds.tsv
        [ -f \"\$f\" ] || { echo \"  i\$k 还没有 rounds.tsv\"; continue; }
        echo \"  i\$k rounds=\$(( \$(wc -l < \$f) - 1 ))  上一轮 \$(( now - \$(stat -c %Y \$f) ))s 前  |  \$(head -1 $RUN_DIR/inst\$k/status 2>/dev/null)\"
    done
    echo \"  崩溃签名: \$(wc -l < $RUN_DIR/crash-signatures.txt 2>/dev/null || echo 0) 条\"
    tail -3 $RUN_DIR/watch.log 2>/dev/null | sed 's/^/  /'"
    exit 0
fi

# ---------------------------------------------------------------- start
[ -n "$FE_HOST" ]  || die "--fe-host 必填"
[ -n "$CORPUS" ]   || die "--corpus 必填"
[ -n "$HARNESS" ]  || die "--harness 必填"
[ -n "$SEED" ]     || SEED=$(date +%Y%m%d)
[ -n "$BE_HTTP" ]  || BE_HTTP="$FE_HOST:8040"
case "$INSTANCES" in ''|*[!0-9]*) die "--instances 必须是数字" ;; esac
[ "$INSTANCES" -ge 1 ] || die "--instances 至少为 1"

MYSQL_CMD="mysql -h$FE_HOST -P$FE_PORT -u$FE_USER"
[ -n "$FE_PASS" ] && MYSQL_CMD="$MYSQL_CMD -p$FE_PASS"

echo "== 预检（能力探测：缺什么就关掉什么，并写进 run.conf）=="

# 1. 服务本身答不答得上查询 -- 不是「进程在不在」。这条规则是这个 campaign 用几百轮空转换来的。
sh_run "timeout 15 $MYSQL_CMD -N -e 'select 1'" >/dev/null 2>&1 \
    || die "连不上 FE：$FE_HOST:$FE_PORT（用户 $FE_USER）"
ok "FE 能答查询"

BE_ALIVE=$(sh_run "timeout 20 $MYSQL_CMD -N -e 'show backends' 2>/dev/null | awk -F'\t' '{print \$9}' | grep -ci true" 2>/dev/null | tr -dc '0-9')
[ "${BE_ALIVE:-0}" -ge 1 ] || die "没有 Alive 的 BE —— 有 pid 不等于在服务"
ok "BE 已注册且 Alive（$BE_ALIVE 个）"

# 2. 语料
NGROUP=$(sh_run "ls '$CORPUS'/*.setup.sql 2>/dev/null | wc -l" | tr -dc '0-9')
[ "${NGROUP:-0}" -gt 0 ] || die "语料目录里没有 *.setup.sql：$CORPUS"
ok "语料 $NGROUP 组"
[ "$NGROUP" -ge "$INSTANCES" ] || die "语料组数 $NGROUP 少于实例数 $INSTANCES"

have "$HARNESS" || die "harness 不存在：$HARNESS"
GEN_DATA=$(dirname "$HARNESS")/gen_data.py
have "$GEN_DATA" || die "gen_data.py 不在 harness 旁边：$GEN_DATA"
ok "harness 与 gen_data.py 就位"

# 3. BE 的内存上限。不设上限的 BE 会把共享机器压到 earlyoom 开火，连邻居一起杀。
MEMLIMIT=$(sh_run "curl -s --max-time 8 http://$BE_HTTP/varz 2>/dev/null | sed -n 's/^mem_limit=//p' | head -1" | tr -d '\r')
if [ -z "$MEMLIMIT" ]; then
    warn "读不到 http://$BE_HTTP/varz —— 无法断言 BE 内存上限（--be-http 指对了吗）"
    MEMLIMIT=unknown
else
    case "$MEMLIMIT" in
        *%) warn "BE mem_limit=$MEMLIMIT 是百分比：共享机器上建议设成绝对值，否则内存尖峰会触发 OOM killer" ;;
        *)  ok "BE mem_limit=$MEMLIMIT" ;;
    esac
fi

# --sr-home may point at a tree holding output/{fe,be} (a build tree) or at the
# deployment itself, holding {fe,be}. Resolve which before deriving anything from it.
SR_LAYOUT=""
if [ -n "$SR_HOME" ]; then
    if [ -d "$SR_HOME/output/be" ]; then SR_LAYOUT="$SR_HOME/output"
    elif [ -d "$SR_HOME/be" ]; then SR_LAYOUT="$SR_HOME"
    fi
fi

# 4. 崩溃预言机：需要读得到 be.out。读不到就必须显式关掉 —— 否则「没有崩溃」和「没在看崩溃」长得一样。
[ -z "$BE_LOG" ] && [ -n "$SR_LAYOUT" ] && BE_LOG="$SR_LAYOUT/be/log/be.out"
[ -n "$BE_LOG" ] && ! have "$BE_LOG" && BE_LOG=""
if [ -n "$BE_LOG" ]; then ok "崩溃预言机启用（$BE_LOG）"; else warn "崩溃预言机：读不到 be.out，本次运行不检测 BE 崩溃"; fi

# 5. FE 日志签名预言机
[ -z "$FE_LOG_DIR" ] && [ -n "$SR_LAYOUT" ] && FE_LOG_DIR="$SR_LAYOUT/fe/log"
[ -n "$FE_LOG_DIR" ] && ! have "$FE_LOG_DIR/fe.warn.log" && FE_LOG_DIR=""
if [ -n "$FE_LOG_DIR" ]; then ok "FE 日志签名预言机启用（$FE_LOG_DIR）"; else warn "FE 日志签名预言机：读不到 fe.warn.log，本次运行不采集 FE 内部异常"; fi

# 6. 自动重启能力
RESTART=0
if [ "$NO_RESTART" = 0 ] && [ -n "$SR_LAYOUT" ] && have "$SR_LAYOUT/be/bin/start_be.sh"; then
    RESTART=1; ok "FE/BE 自动重启启用（$SR_LAYOUT）"
else
    warn "自动重启：未启用 —— 集群挂了需要人工介入（对着别人的集群跑时这是对的）"
fi

echo "== 物化运行目录 $RUN_DIR =="
sh_run "mkdir -p '$RUN_DIR/logs' && for k in \$(seq 0 \$(( $INSTANCES - 1 ))); do mkdir -p '$RUN_DIR'/inst\$k; done"

# 语料用软链而不是复制（动辄几个 G）。注意不能先 mkdir emit 再 ln —— 那样链接会落进目录里面，
# 而 \$RUN_DIR/emit 变成一个空目录，harness 只会说「no corpus」然后退出，看起来像语料没准备好。
sh_run "rm -rf '$RUN_DIR/emit'; ln -sfn '$CORPUS' '$RUN_DIR/emit' || cp -r '$CORPUS' '$RUN_DIR/emit'"

# harness 副本：把写死的两处替换掉。用 python 做，sed 的转义在含斜杠的路径上太脆。
sh_run "python3 - <<'PYEOF'
import re
src = open('$HARNESS').read()
src = re.sub(r'(?m)^W=.*\$',        'W=$RUN_DIR', src, count=1)
src = re.sub(r'(?m)^OUT=.*\$',      'OUT=$RUN_DIR', src, count=1)
src = re.sub(r'(?m)^CORPUS=.*\$',   'CORPUS=$RUN_DIR/emit', src, count=1)
src = re.sub(r'(?m)^MYSQL=.*\$',    'MYSQL=\"$MYSQL_CMD\"', src, count=1)
if '$BE_LOG':   src = re.sub(r'(?m)^BELOG=.*\$', 'BELOG=$BE_LOG', src, count=1)
if '$FE_LOG_DIR':
    src = re.sub(r'(?m)^FELOG=.*\$', 'FELOG=$FE_LOG_DIR/fe.warn.log', src, count=1)
    src = re.sub(r'(?m)^FEDIR=.*\$', 'FEDIR=$FE_LOG_DIR', src, count=1)
if '$SR_LAYOUT':
    # Restart scripts, the FE conf repair and the log readers all derive from it.
    src = src.replace('\$W/output/', '$SR_LAYOUT/')
open('$RUN_DIR/clusterfuzz.run.sh','w').write(src)
PYEOF
chmod +x '$RUN_DIR/clusterfuzz.run.sh'"
# gen_data.py 的连接端点也写死过一次（-P9030）。副本必须跟着这次运行的 FE 走，
# 否则数据灌进另一套集群，而 harness 在本集群上看到「空表」，preflight 会去怪 schema。
sh_run "python3 - <<'PYEOF'
import re, sys
src = open('$GEN_DATA').read()
new = re.sub(r'(?m)^MYSQL = \\[.*\\]$',
             'MYSQL = [\"mysql\", \"-h$FE_HOST\", \"-P$FE_PORT\", \"-u$FE_USER\", \"-N\", \"-B\"]',
             src, count=1)
if new == src:
    sys.exit('gen_data.py: MYSQL endpoint not rewritten -- refusing to run against the wrong cluster')
open('$RUN_DIR/gen_data.py','w').write(new)
PYEOF" || die "gen_data.py 的连接端点改写失败"
sh_run "grep -q -- '-P$FE_PORT' '$RUN_DIR/gen_data.py'" || die "gen_data.py 副本没指向 $FE_HOST:$FE_PORT"
sh_run "bash -n '$RUN_DIR/clusterfuzz.run.sh'" || die "生成的 harness 副本语法不通过"
ok "harness 副本已生成并通过语法检查"

# knob 劈分：每实例只跑一部分，机队合起来仍覆盖全部。单轮成本随实例数下降，覆盖不变。
sh_run "sed -n '/^DIFF_KNOBS=/,/\"}\$/p' '$RUN_DIR/clusterfuzz.run.sh' | tr '|' '\n' \
        | grep -oE 'set [a-z_0-9]+ *= *[^\"|\\\\]*' | sed 's/[[:space:]]*\$//' > '$RUN_DIR/allknobs.txt'"
NKNOB=$(sh_run "wc -l < '$RUN_DIR/allknobs.txt'" | tr -dc '0-9')
if [ "$KNOB_SPLIT" = 1 ] && [ "${NKNOB:-0}" -gt "$INSTANCES" ]; then
    sh_run "for k in \$(seq 0 \$(( $INSTANCES - 1 ))); do
        awk -v k=\$k -v n=$INSTANCES 'NR%n==k' '$RUN_DIR/allknobs.txt' | paste -sd'|' - > \"$RUN_DIR/knobs_\$k.txt\"
    done"
    ok "差分 knob $NKNOB 个，按 $INSTANCES 个实例劈分（每轮成本降到 1/$INSTANCES，机队覆盖不变）"
else
    sh_run "for k in \$(seq 0 \$(( $INSTANCES - 1 ))); do paste -sd'|' - < '$RUN_DIR/allknobs.txt' > \"$RUN_DIR/knobs_\$k.txt\"; done"
    ok "差分 knob $NKNOB 个，每个实例都跑全部"
fi

# 自检。这一步存在的理由：上一版把 emit 建成了空目录、knob 文件名里带着未展开的 $k，
# 而启动器照样打印「劈分完成」。一个不检查自己产物的启动器，产出的就是「看起来启动了」。
SEEN_GROUPS=$(sh_run "ls '$RUN_DIR/emit'/*.setup.sql 2>/dev/null | wc -l" | tr -dc '0-9')
[ "${SEEN_GROUPS:-0}" -gt 0 ] || die "自检失败：$RUN_DIR/emit 里看不到语料（软链没生效？）"
k=0
while [ "$k" -lt "$INSTANCES" ]; do
    n=$(sh_run "tr '|' '\n' < '$RUN_DIR/knobs_$k.txt' 2>/dev/null | grep -c 'set '" | tr -dc '0-9')
    [ "${n:-0}" -gt 0 ] || die "自检失败：knobs_$k.txt 不存在或为空"
    k=$((k + 1))
done
ok "自检通过：语料 $SEEN_GROUPS 组可见，$INSTANCES 份 knob 文件均非空"

RUN_ID="srfuzz-$(basename "$RUN_DIR")"
sh_run "cat > '$CONF' <<EOF
# 本次运行的全部参数与能力判定。发现要能回溯到「当时是什么配置」，否则事后无法解释。
RUN_ID=$RUN_ID
RUN_DIR=$RUN_DIR
RUN_STARTED=\$(date '+%F %T')
RUN_INSTANCES=$INSTANCES
RUN_SEED=$SEED
FE=$FE_HOST:$FE_PORT user=$FE_USER
BE_HTTP=$BE_HTTP
BE_MEM_LIMIT=$MEMLIMIT
CORPUS=$CORPUS ($NGROUP 组)
DIFF_MAX_STMTS=$DIFF_MAX_STMTS
DIFF_KNOBS_TOTAL=$NKNOB knob_split=$KNOB_SPLIT
ORACLE_CRASH=$([ -n "$BE_LOG" ] && echo on || echo OFF)
ORACLE_FE_LOG=$([ -n "$FE_LOG_DIR" ] && echo on || echo OFF)
AUTO_RESTART=$([ "$RESTART" = 1 ] && echo on || echo OFF)
EOF"
ok "run.conf 已写入"

if [ "$DRY_RUN" = 1 ]; then
    echo
    echo "== dry-run：物化完成，未启动实例 =="
    sh_run "cat '$CONF'" | sed 's/^/  /'
    echo
    echo "去掉 --dry-run 即可真正启动。"
    exit 0
fi

echo "== 启动 $INSTANCES 个实例 =="
# 每个实例一个独立的、长期存活的执行会话。绝不从 watchdog 内部用 setsid 拉起：那样实例会挂在
# 别人的会话树上，被反复 SIGTERM 收走，而外表看起来只是「跑得慢」。
k=0
while [ "$k" -lt "$INSTANCES" ]; do
    sh_run_d "cd '$RUN_DIR' && NINSTANCES=$INSTANCES INSTANCE=$k SRFUZZ_GEN_SEED=$SEED \
        DIFF_MAX_STMTS=$DIFF_MAX_STMTS DIFF_KNOBS=\"\$(cat '$RUN_DIR/knobs_$k.txt')\" \
        exec ./clusterfuzz.run.sh >> inst$k/run.stdout 2>> inst$k/run.stderr"
    echo "  实例 $k 已启动"
    k=$((k + 1))
    sleep 2
done

WATCH=$(dirname "$HARNESS")/watch_cf.sh
if have "$WATCH"; then
    # The watchdog polls the BE's /varz and asserts the mem_limit has not been reset.
    # Both the endpoint and the expected value were hardcoded to one particular cluster,
    # so against any other one it alerted forever about a backend this run never touches.
    sh_run "cp '$WATCH' '$RUN_DIR/watch_cf.sh' && sed -i \
        's|^CF=.*|CF=$RUN_DIR|; s|^W=.*|W=${SR_LAYOUT:-$RUN_DIR}|; \
         s|http://127.0.0.1:8040/varz|http://$BE_HTTP/varz|; \
         s|^WANT_MEM_LIMIT=.*|WANT_MEM_LIMIT=\${WANT_MEM_LIMIT:-${MEMLIMIT:-unset}}|' '$RUN_DIR/watch_cf.sh'"
    sh_run "grep -q '$BE_HTTP/varz' '$RUN_DIR/watch_cf.sh'" \
        || die "watchdog 的 /varz 端点没改写成 $BE_HTTP —— 它会对着别的 BE 报警"
    sh_run_d "cd '$RUN_DIR' && NINSTANCES=$INSTANCES INTERVAL=300 SRFUZZ_GEN_SEED=$SEED exec ./watch_cf.sh >> watch.stdout 2>> watch.stderr"
    ok "watchdog 已启动（只告警不自动拉起实例）"
else
    warn "watchdog：$WATCH 不存在，本次运行没有停滞检测"
fi

cat <<EOF

启动完成。判据是轮次计数在涨，不是进程在不在：
  $PROG status --run-dir $RUN_DIR ${CONTAINER:+--container $CONTAINER}
停止：
  $PROG stop   --run-dir $RUN_DIR ${CONTAINER:+--container $CONTAINER}
EOF
