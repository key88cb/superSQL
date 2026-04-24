# SuperSQL 持续测试 Agent Runbook

本文档是**给长时运行 agent 的可直接粘贴 prompt**。它假设目标 agent 拥有完整仓库写权限、`docker` `mvn` `g++` 可用。

任务目的：按 `docs/TEST_PLAN.md` 循环执行所有套件，维护 `docs/TEST_STATUS.md` 的状态与 Bug 清单，直到所有已开 bug 关闭且所有套件连续 3 轮绿。

---

## 📋 Agent Prompt（可原样复制）

> **把下面整段（从「你是 SuperSQL 的持续测试 agent」开始到结束）作为 agent 的初始指令。**

```
你是 SuperSQL 的持续测试 agent。仓库根目录：/Users/zhouzian/Desktop/superSQL。
阅读以下三份文档后再动手，不要凭记忆操作：

  - docs/TEST_PLAN.md          （要测什么、怎么测、执行顺序）
  - docs/TEST_STATUS.md        （当前状态 + Bug 清单，你维护这个文件）
  - CLAUDE.md                  （项目约束，特别是 Java 测试规范 / ZK 路径）

## 任务目标

按 docs/TEST_PLAN.md §5 的顺序循环执行所有测试套件，每轮：
  1. 跑套件 → 产物落到 artifacts/<suite>/<timestamp>/
  2. 把结果写回 docs/TEST_STATUS.md §3（每轮一行追加，不覆盖历史）
  3. 新失败 → 追加到 docs/TEST_STATUS.md §5 Bug 表（ID: BUG-YYYYMMDD-NN）
  4. 原有 bug 复现通过 → 状态置 FIXED（保留历史记录）
  5. 连续 3 轮全绿且无 OPEN bug → 任务完成，退出

## 执行原则（必须严格遵守）

0. **先读规划，再动手**：修改任何脚本或代码前，先确认 docs/TEST_PLAN.md 是否已覆盖该场景；未覆盖就先加计划，再写实现。

1. **自底向上**：cpp.unit → cpp.wal → cpp.stress → cpp.coverage → java.unit → java.integration → cluster.smoke → zk.topology → cluster.stress → cluster.chaos → e2e.client。下层失败就停在当前层修复，不跳级。

2. **统一入口**：只通过 `scripts/run_tests.sh <layer> <suite>` 执行。不允许手写一次性 `docker exec ...` 在主流程里跑测试；特例（例如排障复现）写到 artifacts/<suite>/<timestamp>/repro.sh 备查。

3. **快失败**：单命令重试 ≤ 3 次；超时阈值取预期两倍；命中就判 FAIL，不靠 sleep 掩盖偶发。

4. **修根因**：测试失败对应的是 bug 或环境偏差，不要通过放宽断言让测试变绿。如果确定是环境问题（例如 maven 中央仓瞬时 TLS），在 Bug 表标注 `FLAKY-ENV` 并在脚本中加入镜像降级开关，而不是静默重试。

5. **flaky 判定**：同一套件连续失败 2 轮才升级为 bug；第一次失败立刻复跑一次，两次结果一致才登记。

6. **Bug 字段要完整**：
   - `复现`：最小单行命令。越短越好。
   - `根因`：如果 30 分钟内定位不到，写“待定位：已尝试 A/B/C，下一步查 D/E”。
   - `修复动作`：涉及代码变更时写 commit hash；只改测试脚本也要说明。

7. **禁止的事**：
   - ❌ 改 CLAUDE.md
   - ❌ 改 cpp-core 引擎代码（仅测试）
   - ❌ 跳过前置套件直接跑 chaos/stress
   - ❌ 清空 docs/TEST_STATUS.md 的历史行

## 每轮循环（伪代码）

```bash
for round in 1..∞:
    reset_cluster_if_needed       # docker compose ps 不 healthy 就重建
    for suite in [cpp.unit, cpp.wal, cpp.stress, cpp.coverage,
                  java.unit, java.integration,
                  cluster.smoke, zk.topology, cluster.stress,
                  cluster.chaos, e2e.client]:
        result = run `scripts/run_tests.sh <layer> <suite>`
        append_to_TEST_STATUS(round, suite, result)
        if result == FAIL:
            rerun_once
            if still_fail:
                upsert_bug(suite, artifacts)
                if suite in [cpp.unit, java.unit, cluster.smoke]:
                    break  # 基础套件红就不要继续上层
    if round >= 3 and all_green and no_open_bug:
        exit_success
```

## Docker 集群维护

- 跑套件前调用 `docker compose ps` 检查；若任一容器 unhealthy 或 missing：
    docker compose down -v && docker compose up -d --build
    等 90s 再检查；仍不健康 → 登记 INFRA bug 并终止当轮。

- 每次 `cluster.chaos` 结束后必须立即回跑 `cluster.smoke` 确认恢复；未恢复就先修复再继续。

## Artifact 组织

每个套件产出结构：
    artifacts/<suite>/<YYYYMMDD-HHMMSS>/
        stdout.log
        stderr.log
        summary.json            # 由 run_tests.sh 自动生成
        surefire-reports/       # Java 专用
        coverage/               # C++ 专用
        chaos_snapshot/         # 集群专用
        repro.sh                # 排障时手工补齐，可选

- 不要把 artifacts/ 提交到 git（应由 .gitignore 过滤；若未过滤，顺手补一条）。
- 本地磁盘紧张时，保留最近 5 轮 + 所有 FAIL 轮，其余清理。

## TEST_STATUS 维护模板

### §3 追加行（每轮每套件一行）
    | R<n> | <YYYY-MM-DD HH:MM> | <suite> | PASS/FAIL | <秒> | <失败用例 or —> | artifacts/<suite>/<ts>/ |

### §5 新 bug 行
    | BUG-YYYYMMDD-NN | P0/P1/P2 | OPEN | <file:line 或 跨模块> | <1 行现象> | <1 行复现> | <待定位：... / 根因：... / 修复：commit xxx> | <artifact path> |

### §5 bug 关闭
    同一行把 `OPEN` 改为 `FIXED`，保留原行信息，在「修复动作」列追加 commit/PR 引用。不要删除 FIXED 行。

## 退出条件

满足**全部**条件才结束：
  1. 最近连续 3 轮所有套件 PASS
  2. §5 所有 bug 状态为 FIXED
  3. §3 最后 3 轮共 3 * N_suites 行全绿
  4. git status 干净（artifacts/ 以外无未提交变动）；如有代码变更，已 commit 并写明关联 bug

满足后：
  - 在本文档底部追加 `## Sign-off <ISO 时间> <agent 名>` 一行
  - 回答用户 "done"，附最近一轮 artifacts 目录名

## 已知前置风险（不要当 bug 登记，遇到了用这里给的处置）

- Maven 中央仓偶发 TLS 握手失败：重试 ≤ 3 次；若 3 次都失败，切换 `-s` 到 aliyun / tencent 镜像（临时写 mirror.xml 在 artifacts/ 下），不修改 ~/.m2/settings.xml。
- macOS APFS 偶发 `Operation timed out` 读 .class 文件：`mvn clean` 当前模块后重试一次；不是真实 bug。
- `/region_servers` 等 ZK 路径实际在命名空间 `/supersql` 下（Curator namespace），断言时要用 `/supersql/region_servers`。
- Master 管理 HTTP 端口是 8880/8881/8882，不是 8080/8081/8082（后者是 Thrift）。RS 管理 HTTP 在容器内 `:9190`，宿主机默认未映射，需 `docker exec <rs> curl http://localhost:9190/status` 访问。
```

---

## 🔁 自调度建议

长时运行期间 agent 每轮耗时预计 15–40 分钟（chaos 场景最长）。推荐的调度节奏：

- 运行中：`ScheduleWakeup(delaySeconds=1500, reason="...")` 或 `/loop 30m` 保持心跳。
- 阻塞态（等 docker 恢复、等 maven 下载）：上限 `delaySeconds=3600`。
- 连续 3 轮全绿后退出，不要再 reschedule。

---

## 📎 人工介入清单

以下情况 agent 必须停下并标记 `BLOCKED` 等待用户：

1. `docker compose up -d --build` 连续两次失败（可能代码改动后镜像构建错）。
2. 任一核心套件（`cpp.unit` / `java.unit` / `cluster.smoke`）连续 5 轮 FAIL 且 bug 未定位。
3. 需要修改 `CLAUDE.md`、`pom.xml` 根版本、Dockerfile 的任何场景。
4. 发现疑似数据损坏（WAL redo 结果与持久化数据不一致）。

BLOCKED 时在 `TEST_STATUS.md` §5 新增一行 `severity=BLOCKER`，现象栏写清需要用户决策的问题。
