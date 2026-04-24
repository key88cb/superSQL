#!/usr/bin/env bash
# zk.topology — 断言 ZooKeeper 命名空间结构与 docs/CLAUDE.md 一致
#
# /masters              临时顺序节点，>=1（通常 3）
# /active-master        持久节点，JSON 含 epoch
# /region_servers       >=1（通常 3）
# /meta/tables          0..N（DDL 后会存在）
# /assignments          同 /meta/tables

set -u
set -o pipefail

ZK_CLI="${ZK_CLI:-docker exec zk1 zkCli.sh -server zk1:2181}"

fail() { echo "[FAIL] $*" >&2; exit 1; }
ok()   { echo "[PASS] $*"; }

zk_ls() {
    # $1 path — prints children count on success
    $ZK_CLI ls "$1" 2>/dev/null | tail -n +1 | grep -oE '\[[^]]*\]' | tr -d '[]' | tr ',' '\n' | sed '/^$/d' | wc -l | tr -d ' '
}

zk_get() {
    $ZK_CLI get "$1" 2>/dev/null | tail -n 1
}

# 1. /masters
masters_count=$(zk_ls /masters)
[[ "$masters_count" -ge 1 ]] || fail "/masters children=$masters_count"
ok "/masters children=$masters_count"

# 2. /active-master
active=$(zk_get /active-master)
[[ -n "$active" ]] || fail "/active-master empty"
echo "$active" | grep -q 'epoch' || fail "/active-master payload missing 'epoch': $active"
ok "/active-master payload=$active"

# 3. /region_servers
rs_count=$(zk_ls /region_servers)
[[ "$rs_count" -ge 1 ]] || fail "/region_servers children=$rs_count"
ok "/region_servers children=$rs_count"

# 4. /meta/tables — optional（无表时为空）
tables_count=$(zk_ls /meta/tables 2>/dev/null || echo 0)
ok "/meta/tables children=$tables_count (informational)"

# 5. /assignments — optional
assign_count=$(zk_ls /assignments 2>/dev/null || echo 0)
ok "/assignments children=$assign_count (informational)"

# 一致性：若 /meta/tables 非空，每张表应同时存在于 /assignments
if [[ "$tables_count" -gt 0 ]]; then
    $ZK_CLI ls /meta/tables 2>/dev/null | tail -n +1 | grep -oE '\[[^]]*\]' | tr -d '[]' | tr ',' '\n' | sed '/^$/d' | while read -r t; do
        t_trim=$(echo "$t" | tr -d ' ')
        [[ -z "$t_trim" ]] && continue
        $ZK_CLI get "/assignments/$t_trim" >/dev/null 2>&1 \
            || fail "/meta/tables/$t_trim exists but /assignments/$t_trim missing"
    done
    ok "meta ↔ assignments consistency"
fi

echo "[SUMMARY] zk.topology PASS"
