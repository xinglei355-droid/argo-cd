# `argocd app sync` 完整调用链分析

本文聚焦以下代码路径，说明一次用户执行 `argocd app sync` 后，Argo CD 从 API Server 接收请求、校验、repo-server 生成 manifests、application-controller 对比 live state、执行同步、再到回写 `Application.status` 的完整链路：

- [application.go](file:///app/argo-cd/server/application/application.go)
- [appcontroller.go](file:///app/argo-cd/controller/appcontroller.go)
- [repository.go](file:///app/argo-cd/reposerver/repository/repository.go)
- [cache.go](file:///app/argo-cd/controller/cache/cache.go)
- [types.go](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go)

为了把调用链串完整，文中也补充引用了少量直接被这些文件调用的邻近代码：

- [application.proto](file:///app/argo-cd/server/application/application.proto#L125-L140)
- [sync.go](file:///app/argo-cd/controller/sync.go#L101-L401)
- [state.go](file:///app/argo-cd/controller/state.go#L202-L1335)
- [argo.go](file:///app/argo-cd/util/argo/argo.go#L938-L958)

## 1. 先看几个核心对象

| 结构体 | 位置 | 在调用链中的职责 |
| --- | --- | --- |
| `Application` | [types.go:L68-L73](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L68-L73) | 整个流程的中心对象；`spec` 是期望态，`status` 是观测态，`operation` 是“待执行的请求” |
| `ApplicationStatus` | [types.go:L1208-L1241](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1208-L1241) | controller 最终回写的位置，承载 `sync`、`health`、`resources`、`conditions`、`history`、`operationState` 等 |
| `Operation` | [types.go:L1342-L1352](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1342-L1352) | API Server 把一次 sync 请求落到 `application.operation` 时使用的封装 |
| `SyncOperation` | [types.go:L1410-L1443](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1410-L1443) | 描述这次 sync 的参数：revision、prune、dryRun、resources、sources、manifests、syncOptions |
| `OperationState` | [types.go:L1445-L1459](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1445-L1459) | application-controller 执行中的运行态，记录 `phase`、`message`、`syncResult`、开始/结束时间、重试计数 |
| `SyncOperationResult` | [types.go:L1751-L1765](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1751-L1765) | 一次 sync 的结果快照，记录实际执行到的 revision(s)、resource 结果、managed namespace metadata |
| `RevisionHistory` | [types.go:L1827-L1848](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1827-L1848) | 成功完成非 dry-run 全量 sync 后写入历史 |
| `ComparedTo` / `SyncStatus` | [types.go:L1924-L1946](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1924-L1946) | reconciliation 对比结果，记录“拿什么 source/destination/revision 做的比较”以及最终是 `Synced/OutOfSync/Unknown` |
| `ApplicationCondition` | [types.go:L1914-L1922](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1914-L1922) | 比较错误、非法配置、共享资源告警、自动同步错误等都通过 condition 暴露 |
| `LiveStateCache` | [cache.go:L134-L151](file:///app/argo-cd/controller/cache/cache.go#L134-L151) | controller 访问集群 live state 的统一入口，提供 live objects、API 资源列表、cluster cache |

一个非常关键但容易混淆的点：

- `application.operation` 表示“用户/系统希望 controller 去执行什么”
- `status.operationState` 表示“controller 当前把这件事执行到了哪里”
- `status.sync` / `status.health` / `status.resources` 表示“controller 经过完整 reconciliation 后认定的当前应用状态”

## 2. 总览：一条完整时序链

一次手工 `argocd app sync`，从宏观上会经过下面几个阶段：

1. CLI 调用 Argo CD API 的 `ApplicationService.Sync` gRPC 接口
2. `argocd-server` 在 [Sync](file:///app/argo-cd/server/application/application.go#L2069-L2165) 中做 RBAC、sync window、revision 解析等校验
3. API Server 将请求写入 `Application.operation`
4. `application-controller` 从 operation queue 取出该应用，初始化/恢复 `status.operationState`
5. controller 在 [SyncAppState](file:///app/argo-cd/controller/sync.go#L101-L401) 里先做一次 `CompareAppState`
6. `CompareAppState` 通过 repo-server 获取 manifests，并通过 liveStateCache 获取 live objects，完成 diff / sync status / health 计算
7. controller 构造 gitops-engine `SyncContext`，真正执行 apply/prune/hook
8. controller 更新 `status.operationState`，并在成功结束后清空 `application.operation`
9. controller 再通过 refresh/reconciliation 路径做一次完整状态刷新，把 `status.sync`、`status.health`、`status.resources`、`status.conditions`、`status.history` 等回写到 CR

其中第 5 步和第 9 步都会“比较 live state”，但目的不同：

- 第 5 步是“执行 sync 之前，为本次操作构建目标对象集和差异信息”
- 第 9 步是“sync 完成后，刷新最终展示给用户的应用状态”

## 3. API Server 接收请求与前置校验

### 3.1 gRPC 入口

`argocd app sync` 对应的请求体定义在 [application.proto:L125-L140](file:///app/argo-cd/server/application/application.proto#L125-L140)，服务端处理函数是 [Sync](file:///app/argo-cd/server/application/application.go#L2069-L2165)。

请求中的关键字段包括：

- `name` / `appNamespace` / `project`
- `revision` 或多源场景下的 `sourcePositions` + `revisions`
- `dryRun` / `prune`
- `strategy`
- `resources`（局部同步）
- `manifests`（本地 manifest 覆盖）
- `syncOptions`
- `retryStrategy`

### 3.2 读取 Application 并做 RBAC

`Sync()` 第一件事就是调用 [getApplicationEnforceRBACClient](file:///app/argo-cd/server/application/application.go#L269-L289)，后者再进入 [getAppEnforceRBAC](file:///app/argo-cd/server/application/application.go#L177-L247)。这一段的职责是：

- 从 Kubernetes API 读取最新 `Application`
- 基于请求中给定的 `project/namespace/name` 做首轮 RBAC
- 读取到真实应用后，再用真实 `a.RBACName()` 做第二轮 RBAC
- 在未显式给出 `project` 时，把“不存在”和“无权限”都模糊成 permission denied，避免泄漏对象是否存在
- 顺便取回该应用所属的 `AppProject`

这里的返回值不是只有 `Application`，而是 `Application + AppProject`，因为后续 sync window、源仓库权限、目标集群权限都依赖 project。

### 3.3 Sync() 内的业务校验

`Sync()` 在真正创建 operation 之前，还会做几类关键校验：[application.go:L2076-L2139](file:///app/argo-cd/server/application/application.go#L2076-L2139)

- `proj.Spec.SyncWindows.Matches(a).CanSync(true, nil)`：手工 sync 是否被 sync window 阻止
- `rbac.ActionSync`：是否拥有 sync 权限
- 如果请求携带 `manifests`，额外校验 `rbac.ActionOverride`
- 自动同步开启时，禁止非 dry-run 的 local manifests sync
- 删除中的应用禁止 sync
- `SyncOptionReplace` 可能被 API Server 配置整体禁用
- source integrity 开启时禁止 local manifests

### 3.4 解析 revision：API Server 到 repo-server 的第一次 gRPC

`Sync()` 随后调用 [resolveSourceRevisions](file:///app/argo-cd/server/application/application.go#L2167-L2254)。它的职责有两层：

- 决定这次 sync 使用哪个 revision / revisions
- 判定“这次请求是否试图覆盖 app spec 中原本声明的 revision”，必要时要求 `override` 权限

真正把 `HEAD` / branch / tag / chart version 解析成具体 revision 的逻辑在 [resolveRevision](file:///app/argo-cd/server/application/application.go#L2448-L2487)。这里会：

- 根据 sourceIndex 找到对应 source
- 读取 repo 配置
- 通过 `s.repoClientset.NewRepoServerClient()` 建立到 repo-server 的 gRPC 客户端
- 调用 repo-server 的 [ResolveRevision](file:///app/argo-cd/reposerver/repository/repository.go#L3106-L3140)

也就是说，**用户发起 sync 时，API Server 就已经可能先向 repo-server 做了一次 revision 解析**，这一步还没有生成 manifests，只是为了把请求固定到一个具体 revision。

### 3.5 写入 `application.operation`

校验完成后，`Sync()` 会组装一个 [Operation](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1342-L1352) 和 [SyncOperation](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1410-L1443)，再调用 [SetAppOperation](file:///app/argo-cd/util/argo/argo.go#L938-L958)。

这一写入有几个重要副作用：

- 将 `Application.operation` 设置为新的 sync 请求
- 将 `status.operationState` 清空
- 如果已有 `operation`，直接返回 `ErrAnotherOperationInProgress`
- 成功后 API Server 记录审计事件并返回

这一时刻，**sync 还没有真正开始执行**；真正执行是在 application-controller 看到 `operation` 后。

## 4. application-controller 接管 operation

### 4.1 operation queue 入口

controller 处理入口是 [processAppOperationQueueItem](file:///app/argo-cd/controller/appcontroller.go#L1000-L1053)。它会：

- 从 operation queue 取出应用 key
- 尽量读取 informer 中对象
- 如果 `app.Operation != nil`，再额外从 Kubernetes API 拉一遍 fresh app，避免基于陈旧缓存执行 sync
- 把 fresh app 交给 [processRequestedAppOperation](file:///app/argo-cd/controller/appcontroller.go#L1430-L1584)

### 4.2 初始化或恢复 `status.operationState`

`processRequestedAppOperation()` 负责把“用户请求”转换为“正在执行的运行态”：

- 若当前已有 in-progress `status.operationState`，则走恢复/超时/重试逻辑
- 否则调用 `NewOperationState(*app.Operation)` 初始化 `phase=Running`
- 立即调用 [setOperationState](file:///app/argo-cd/controller/appcontroller.go#L1586-L1676) 把运行态 patch 到 `status.operationState`
- 然后加载 project，进入 [SyncAppState](file:///app/argo-cd/controller/sync.go#L101-L401)

`setOperationState()` 的行为很关键：

- `state.Phase.Completed()` 时自动补 `finishedAt`
- patch `status.operationState`
- 若 operation 已完成，额外把顶层 `operation` 字段清空
- 记录 event / metrics
- sync 结束后发起一次 app refresh 请求，用于刷新最终的 `status.sync` / `status.health`

因此，**operation 执行路径和 status 刷新路径是分开的**：

- operation 路径主要更新 `status.operationState`
- refresh 路径主要更新 `status.sync`、`status.health`、`status.resources` 等

## 5. Sync 前半段：CompareAppState 如何生成目标状态并比对 live state

真正执行 sync 之前，controller 会先跑 [SyncAppState](file:///app/argo-cd/controller/sync.go#L101-L401)。

### 5.1 SyncAppState 的前置校验

`SyncAppState()` 里先做几件事：

- 生成 `syncId` 便于日志串联
- 检查 `state.Operation.Sync` 不能为空
- 初始化 `state.SyncResult`，调用 `newSyncOperationResult()` 把本次 sync 使用的 source/revision 记录下来
- 再次检查 sync window
- 从 `state.SyncResult` 中整理出单源或多源的 `sources` 与 `revisions`

这里要注意：**controller 不直接相信 app.spec 当前值，而是优先依据 operationState / syncResult 中固定下来的 revision(s)**，保证恢复中的 sync 使用同一组目标 revision。

### 5.2 CompareAppState：controller 的核心比较入口

随后进入 [CompareAppState](file:///app/argo-cd/controller/state.go#L629-L1189)。它是整个链路里最关键的状态计算函数之一，负责产出：

- `syncStatus`
- `healthStatus`
- `resources` / `managedResources`
- `reconciliationResult`
- `diffResultList`
- 各类 `conditions`
- 是否存在 `PreDelete` / `PostDelete` hooks

其内部可拆成几个阶段。

### 5.3 构建初始 `SyncStatus`

`CompareAppState()` 一开始先构造一个初始 [SyncStatus](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1936-L1946)：

- `ComparedTo.Destination = app.Spec.Destination`
- `ComparedTo.IgnoreDifferences = app.Spec.IgnoreDifferences`
- 单源时填 `ComparedTo.Source + Revision`
- 多源时填 `ComparedTo.Sources + Revisions`
- 初始状态先设为 `Unknown`

这一步的意义是：即使后续 repo 或 live state 读取失败，也至少有一份“本次在比较什么”的上下文可落到状态里。

### 5.4 读取 comparison settings

`CompareAppState()` 调用 `getComparisonSettings()`，拿到：

- app instance label key
- resource overrides
- resources filter
- installation ID
- tracking method

这些设置会直接影响：

- repo-server manifest 生成时是否注入 tracking 信息
- live 对象筛选和归属判断
- diff 时忽略哪些字段
- 哪些资源会被 settings 排除

### 5.5 获取目标对象：GetRepoObjs

如果不是 local manifests，同步前的目标对象来自 [GetRepoObjs](file:///app/argo-cd/controller/state.go#L202-L459)。这一层做的事情很多：

- 从 DB 读取 Helm/OCI 仓库和凭据，并按 project 过滤
- 读取 enabled source types、Kustomize settings、Helm settings、tracking method、installation ID
- 解析目标集群，并通过 [GetVersionsInfo](file:///app/argo-cd/controller/cache/cache.go#L739-L744) 获取：
  - Kubernetes server version
  - API resources 列表
- 建立 repo-server gRPC 客户端
- 解析多源 `refSources`
- 对每个 source 做 revision 变化评估，然后调用 repo-server 生成 manifests

这里有一个很关键的性能优化路径：`evaluateRevisionChanges()` 会优先尝试 repo-server 的 `UpdateRevisionForPaths` / `ResolveRevision`，尽量判断“revision 虽然移动了，但 manifest 实际是否变化”。

### 5.6 controller 到 repo-server 的核心 gRPC 关系

在本次链路中，controller 访问 repo-server 主要有三类 RPC：

1. `ResolveRevision`
   - API Server 的 [resolveRevision](file:///app/argo-cd/server/application/application.go#L2448-L2487) 会调用一次
   - controller 的 `evaluateRevisionChanges()` 在需要强制解析 revision 时也会调用
   - 服务端实现在 [repository.go:L3106-L3140](file:///app/argo-cd/reposerver/repository/repository.go#L3106-L3140)

2. `UpdateRevisionForPaths`
   - controller 在有 `manifest-generate-paths` 和已知 synced revision 时调用
   - 用于判断相关路径是否有变化，并在“无变化但 revision 前移”时搬运 manifest cache
   - 服务端实现在 [repository.go:L3393-L3513](file:///app/argo-cd/reposerver/repository/repository.go#L3393-L3513)

3. `GenerateManifest`
   - controller 在 [GetRepoObjs](file:///app/argo-cd/controller/state.go#L202-L459) 中对每个 source 调用
   - 服务端实现在 [GenerateManifest](file:///app/argo-cd/reposerver/repository/repository.go#L631-L710)

### 5.7 repo-server 如何生成 manifests

repo-server 的 [GenerateManifest](file:///app/argo-cd/reposerver/repository/repository.go#L631-L710) 流程可以概括为：

- 对 ref-only source 直接短路，仅返回 revision
- 先按 manifest cache key 查缓存
- 若缓存命中直接返回；否则通过 `runRepoOperation()` 执行实际 repo checkout / lock / generation
- `runManifestGenAsync()` 中处理多源 ref、checkout 引用仓库、symlink 安全检查、调用 `GenerateManifests()` 生成 manifests
- 生成成功后把结果写回 manifest cache
- 某些错误会被转换为更明确的 gRPC status code，例如 `GlobNoMatchError -> codes.NotFound`

这里 repo-server 返回给 controller 的 `ManifestResponse` 不只是 `manifests`，还包括：

- `Revision`：实际解析后的 commit SHA / chart version
- `SourceType`
- `SourceIntegrityResult`

这些信息后面会被 controller 继续用于：

- 更新 `status.sync.revision(s)`
- 设置应用 source type
- 生成 comparison conditions

### 5.8 CompareAppState 如何对比 live state

拿到 target manifests 后，`CompareAppState()` 会继续：

- 调用 `NormalizeTargetObjects()` 做 namespace 补齐、去重、tracking 信息注入
- 基于 settings 过滤 excluded resources
- 调用 [GetManagedLiveObjs](file:///app/argo-cd/controller/cache/cache.go#L729-L737) 从 liveStateCache 取出该应用管理的 live objects
- 对 live objects 按 project 权限再过滤一次
- 检查 shared resource / managed namespace 等特殊场景
- 调用 `sync.Reconcile(targetObjsForSync, liveObjByKey, ...)` 做 target-live 对齐
- 调用 `argodiff.StateDiffs(...)` 生成逐资源 diff
- 汇总出每个资源的 `ResourceStatus`
- 汇总出应用级别的 `SyncStatusCode` 和 `HealthStatus`
- 把 comparison / shared / repeated / excluded 等 conditions 写入 `app.Status.Conditions`

这一阶段 live state 相关的数据来源都不是直接查 Kubernetes，而是通过 [LiveStateCache](file:///app/argo-cd/controller/cache/cache.go#L134-L151)：

- [GetManagedLiveObjs](file:///app/argo-cd/controller/cache/cache.go#L729-L737)：返回当前应用管理的 live objects
- [GetVersionsInfo](file:///app/argo-cd/controller/cache/cache.go#L739-L744)：给 repo-server 传递运行时 API 能力信息
- [GetClusterCache](file:///app/argo-cd/controller/cache/cache.go#L924-L926)：提供 GVK parser、namespaced 判断等辅助能力

## 6. Sync 后半段：真正执行 apply/prune/hook

回到 [SyncAppState](file:///app/argo-cd/controller/sync.go#L101-L401)，拿到 `compareResult` 后会继续走真正的同步逻辑。

### 6.1 执行前的失败短路

在创建 sync context 前，有几类错误会直接把本次 operation 终止：

- `FailOnSharedResource=true` 且 `app.Status.Conditions` 中已有共享资源告警
- `ApplicationConditionComparisonError` 或 `ApplicationConditionInvalidSpecError` 非空
- 目标集群无法解析
- REST config 获取失败
- resource overrides / settings 加载失败
- `RespectIgnoreDifferences=true` 场景下 target 归一化失败
- impersonation 配置失败
- `sync.NewSyncContext()` 初始化失败

这些错误都会把 `state.Phase` 置为 `OperationError`，`state.Message` 写入具体原因。

### 6.2 SyncContext 的输入

`SyncAppState()` 构造 gitops-engine `SyncContext` 时，关键输入包括：

- `compareResult.syncStatus.Revision`
- `compareResult.reconciliationResult`
- 目标集群的 `restConfig` / `rawConfig`
- kubectl 实现
- 一系列 sync options

特别重要的 option 有：

- `WithPermissionValidator`：按 project 校验目标资源是否允许同步
- `WithOperationSettings`：dry-run、prune、force、partial sync 等参数
- `WithResourcesFilter`：只同步 operation 指定的资源，且过滤掉不属于当前 app 的对象
- `WithManifestValidation`
- `WithPruneLast`
- `WithResourceModificationChecker(ApplyOutOfSyncOnly=true)`
- `WithReplace`
- `WithServerSideApply`
- `WithClientSideApplyMigration`
- `WithPruneConfirmed`
- `WithSkipDryRunOnMissingResource`
- `WithNamespaceModifier(syncNamespace(...))`（CreateNamespace=true 时）

### 6.3 实际执行与结果回填

执行阶段很直接：

- 正常执行调用 `syncCtx.Sync()`
- 终止中的 operation 调用 `syncCtx.Terminate()`
- 然后通过 `syncCtx.GetState()` 取回：
  - `state.Phase`
  - `state.Message`
  - 每个资源的 `ResourceSyncResult`

接着 controller 会把 gitops-engine 结果映射回 `OperationState.SyncResult`：

- `state.SyncResult.Resources = []*ResourceResult`
- `state.SyncResult.Revision/Revisions` 使用 compare 后得到的 concrete revision
- 如配置了 managed namespace metadata，也写入 `state.SyncResult.ManagedNamespaceMetadata`

如果本次 sync：

- 不是 dry-run
- 不是 partial sync
- `state.Phase.Successful()`

则调用 [persistRevisionHistory](file:///app/argo-cd/controller/state.go#L1192-L1235) 把 revision/source/initiatedBy/startedAt 等信息追加到 `status.history`。

## 7. Operation 状态如何写回

`SyncAppState()` 返回后，仍由 [processRequestedAppOperation](file:///app/argo-cd/controller/appcontroller.go#L1430-L1584) 收尾，并最终调用 [setOperationState](file:///app/argo-cd/controller/appcontroller.go#L1586-L1676)。

这一写回会产生几个用户最容易观察到的变化：

- `status.operationState.phase` 从 `Running` 变成 `Succeeded/Failed/Error/Terminating`
- `status.operationState.message` 更新为最终结果
- `status.operationState.syncResult` 挂上 resource 级结果
- `status.operationState.finishedAt` 在完成态自动补齐
- 顶层 `application.operation` 被清空，表示没有 in-flight operation 了

这里可以把它理解成：

- `application.operation` 是“待办”
- `status.operationState` 是“执行日志”

## 8. Sync 完成后，如何回写 `status.sync` / `status.health` / `status.resources`

这一部分不在 `SyncAppState()` 里完成，而在 application-controller 的 refresh 路径完成。入口是 [processAppRefreshQueueItem](file:///app/argo-cd/controller/appcontroller.go#L1691-L1934)。

### 8.1 为什么 sync 后还要再 refresh 一次

`setOperationState()` 在 operation 完成时会调用 `requestAppRefresh()`，原因很明确：

- sync 执行完成时，`status.operationState` 已经是最终态
- 但 `status.sync/status.health/resources` 仍可能还是旧值
- 因此 controller 需要再做一次完整 reconciliation，确保 UI/API 看到的是“已经同步后的最终状态”

### 8.2 refresh 路径做了什么

`processAppRefreshQueueItem()` 的主流程是：

1. 调用 `needRefreshAppStatus()` 判断是否真的需要刷新
2. 调用 [refreshAppConditions](file:///app/argo-cd/controller/appcontroller.go#L2068-L2086) 重新校验 app spec / project 权限
3. 获取目标集群
4. 取出 `status.operationState.operation.sync.manifests` 作为 local manifests（若有）
5. 按单源/多源整理 `revisions` 与 `sources`
6. 再调用一次 [CompareAppState](file:///app/argo-cd/controller/state.go#L629-L1189)
7. 基于 compare 结果更新缓存中的 resource tree / managed resources
8. 更新 `app.Status` 各字段
9. 调用 [persistReconciliationStatus](file:///app/argo-cd/controller/appcontroller.go#L2129-L2134) -> [persistAppStatus](file:///app/argo-cd/controller/appcontroller.go#L2137-L2286) 持久化

### 8.3 这一轮会更新哪些字段

在 [appcontroller.go:L1875-L1931](file:///app/argo-cd/controller/appcontroller.go#L1875-L1931) 可以看到 refresh 路径会显式回填：

- `status.reconciledAt`
- `status.sync`
- `status.health.status`
- `status.resources`
- `status.sourceType` / `status.sourceTypes`
- `status.controllerNamespace`
- `status.summary`
- pre/post-delete finalizer 相关处理

同时 `persistAppStatus()` 还会处理：

- `status.health.lastTransitionTime`
- annotations 中 refresh 标记的清理
- status patch 大小超过 Kubernetes 限制时，降级成只写一个 `UnknownError` condition

换句话说，**用户在 UI 里最终看到的“Synced/OutOfSync、Healthy/Degraded、资源列表、conditions”，主要来自 refresh 路径，而不是 operation 路径。**

## 9. gRPC 调用关系总表

本次链路涉及的 gRPC 关系可以总结如下：

| 调用方 | 被调方 | RPC | 用途 |
| --- | --- | --- | --- |
| `argocd` CLI | `argocd-server` | `ApplicationService.Sync` | 发起一次 app sync 请求，携带 revision/prune/dryRun/syncOptions 等 |
| `argocd-server` | `repo-server` | `ResolveRevision` | 在创建 operation 之前，把用户输入的 branch/tag/HEAD 解析成具体 revision |
| `application-controller` | `repo-server` | `UpdateRevisionForPaths` | 快速判断相关路径是否有变化，并在无变化时迁移 manifest cache |
| `application-controller` | `repo-server` | `ResolveRevision` | 某些场景下强制解析 revision |
| `application-controller` | `repo-server` | `GenerateManifest` | 为每个 source 生成目标 manifests |

另外还有一条容易忽略但很重要的“非 gRPC 写回链路”：

- `argocd-server` 与 `application-controller` 都是通过 Kubernetes API 更新 `Application` CR
- 它们之间不直接 RPC 交接 sync 执行结果，而是通过 `Application.operation` / `status.operationState` / `status.*` 完成协作

## 10. 错误传播路径

下面按阶段总结错误是如何传播的。

### 10.1 API Server 阶段

`Sync()` 中的错误通常会直接作为 gRPC 错误返回给客户端：

- RBAC 不通过：permission denied / not found（取决于是否显式传 project）
- sync window 阻止：`codes.PermissionDenied`
- 删除中应用：`codes.FailedPrecondition`
- local manifests 与 auto-sync/source integrity 冲突：`codes.FailedPrecondition`
- revision 解析失败：包装成 `error resolving repo revision: ...`
- `SetAppOperation` 失败：包装成 `error setting app operation: ...`

这类错误的特点是：**operation 根本没有写进去，controller 不会开始执行**。

### 10.2 repo-server 阶段

repo-server 里的错误会经由 controller / server 继续向上层传播：

- `ResolveRevision` 失败：直接回到 API Server 或 controller
- `GenerateManifest` 失败：在 `CompareAppState()` 中转成 `ApplicationConditionComparisonError`
- 特定错误会被转换成 gRPC status code，例如 `GlobNoMatchError -> NotFound`
- manifest generation 失败还可能进入 repo-server 的“错误缓存”路径，随后一段时间内直接返回 cached generation error

### 10.3 CompareAppState 阶段

`CompareAppState()` 对错误的处理方式不是单一的“直接 return error”，而是分层：

- repo 侧临时错误在 grace period 内可能短路为 `ErrCompareStateRepo`，调用方会忽略这次刷新，避免状态抖动
- 目标对象加载失败、live state 获取失败、diff 失败等，通常会落成 `ApplicationConditionComparisonError`
- 即使部分失败，函数也尽量返回一个带 `Unknown` / 部分条件的 `comparisonResult`

这也是为什么很多时候 UI 里看到的是：

- `status.sync.status = Unknown`
- 同时带一个 `ComparisonError`

而不是整个请求直接失败。

### 10.4 SyncAppState 阶段

`SyncAppState()` 的错误最终主要落到 `status.operationState`：

- 配置错误 / 目标集群错误 / SyncContext 初始化失败：`OperationError`
- `FailOnSharedResource=true` 命中共享资源：`OperationFailed`
- `persistRevisionHistory()` 失败：即使同步动作本身成功，也会把本次 operation 置为 `OperationError`

### 10.5 status 持久化阶段

`setOperationState()` 和 `persistAppStatus()` 本身的 patch 失败也会出现在 controller 日志里：

- `setOperationState()` 会重试 patch `status.operationState`
- `persistAppStatus()` patch 失败会打 warning
- 如果 status 太大，controller 会降级写一个 `UnknownError` condition，提示当前 status 可能陈旧

## 11. 关键状态字段的变化轨迹

下面按时间顺序看几个最关键字段怎么变化。

### 11.1 `application.operation`

- API Server 的 `Sync()` 创建 [Operation](file:///app/argo-cd/pkg/apis/application/v1alpha1/types.go#L1342-L1352)
- [SetAppOperation](file:///app/argo-cd/util/argo/argo.go#L938-L958) 把它写入 `application.operation`
- operation 完成后，[setOperationState](file:///app/argo-cd/controller/appcontroller.go#L1586-L1676) 会把该字段清空

### 11.2 `status.operationState`

- `processRequestedAppOperation()` 初始化为 `phase=Running`
- `SyncAppState()` 更新其中的 `syncResult`、`phase`、`message`
- 完成时补 `finishedAt`
- 如果超时/终止/重试，也都首先反映在这里

### 11.3 `status.sync`

- 由 refresh/reconcile 路径统一写入
- `ComparedTo` 记录本次比较使用的 source/destination/ignoreDifferences
- `Revision/Revisions` 记录 repo-server 最终解析后的具体 revision
- `Status` 取值为 `Synced/OutOfSync/Unknown`

### 11.4 `status.resources`

- 由 `CompareAppState()` 基于 target/live/diff 生成
- 逐资源记录 group/kind/name/namespace/status/hook/prune 等信息
- 最终在 refresh 路径中整体写回

### 11.5 `status.conditions`

来源分三类：

- `refreshAppConditions()`：spec/project 权限类问题
- `CompareAppState()`：comparison/shared/repeated/excluded 等问题
- `autoSync()`：自动同步失败类问题

### 11.6 `status.history`

- 仅在成功的非 dry-run 非 partial sync 后写入
- 记录 revision/source/sources/initiatedBy/deployStartedAt/deployedAt
- 用于 rollback 和审计

## 12. 排查入口建议

如果线上要排查“为什么一次 `argocd app sync` 没按预期工作”，我会按下面顺序看。

### 12.1 先看 API Server 是否接受了请求

重点看：

- [Sync](file:///app/argo-cd/server/application/application.go#L2069-L2165)
- [resolveSourceRevisions](file:///app/argo-cd/server/application/application.go#L2167-L2254)
- [resolveRevision](file:///app/argo-cd/server/application/application.go#L2448-L2487)

典型现象：

- CLI 立刻报 `permission denied` / `not found` / `failed precondition`
- `Application.operation` 根本没有写进去

优先排查：

- RBAC
- sync window
- auto-sync + override/local manifests 限制
- revision 是否能在 repo-server 正常解析

### 12.2 再看 `Application.operation` 与 `status.operationState`

如果请求已被接受：

- `application.operation` 应先出现
- controller 接管后，`status.operationState.phase` 应变为 `Running`
- 结束后 `application.operation` 应被清空

如果 `application.operation` 长时间存在但 `status.operationState` 不推进，优先看：

- application-controller 是否消费到 operation queue
- controller 是否能拿到 fresh app
- controller 是否在进入 `SyncAppState()` 前就报错

### 12.3 看 repo-server manifest 生成

重点函数：

- [GetRepoObjs](file:///app/argo-cd/controller/state.go#L202-L459)
- [GenerateManifest](file:///app/argo-cd/reposerver/repository/repository.go#L631-L710)
- [UpdateRevisionForPaths](file:///app/argo-cd/reposerver/repository/repository.go#L3393-L3513)
- [ResolveRevision](file:///app/argo-cd/reposerver/repository/repository.go#L3106-L3140)

典型信号：

- `ComparisonError: Failed to load target state`
- repo-server 日志出现 manifest cache hit/error cache hit
- ref source / valueFiles / chart / path 解析异常

### 12.4 看 live state 与 diff

重点函数：

- [GetManagedLiveObjs](file:///app/argo-cd/controller/cache/cache.go#L729-L737)
- [CompareAppState](file:///app/argo-cd/controller/state.go#L629-L1189)

典型问题：

- 目标集群拿不到
- live state cache 未同步
- project 不允许访问某些 live resources
- shared resource / repeated resource / excluded resource 导致 condition
- diff 异常导致 `status.sync.status = Unknown`

### 12.5 看 sync 执行本身

重点函数：

- [SyncAppState](file:///app/argo-cd/controller/sync.go#L101-L401)
- [setOperationState](file:///app/argo-cd/controller/appcontroller.go#L1586-L1676)

典型现象：

- `status.operationState.phase = Failed/Error/Terminating`
- `status.operationState.syncResult.resources[]` 中某个 hook 或 apply 失败
- prune / namespace create / SSA / replace / partial sync 行为与预期不一致

### 12.6 最后看 refresh 是否把最终状态刷回去了

重点函数：

- [processAppRefreshQueueItem](file:///app/argo-cd/controller/appcontroller.go#L1691-L1934)
- [persistReconciliationStatus](file:///app/argo-cd/controller/appcontroller.go#L2129-L2134)
- [persistAppStatus](file:///app/argo-cd/controller/appcontroller.go#L2137-L2286)

典型现象：

- operation 已成功，但 UI 还显示旧的 `sync/health`
- `status.resources` 没更新
- `status` 太大导致 patch 降级

## 13. 一句话总结

一次 `argocd app sync` 的本质是：

1. `argocd-server` 负责把用户请求校验并落成 `Application.operation`
2. `application-controller` 负责把这条 operation 执行出来，并将执行过程写入 `status.operationState`
3. controller 再通过一次完整 reconciliation，把 repo-server 生成的目标 manifests 与 live state 重新对比，并把最终结果写入 `status.sync`、`status.health`、`status.resources`、`status.conditions`、`status.history`

所以如果把整条链路压缩成最关键的一组函数，就是：

- API 入口：[Sync](file:///app/argo-cd/server/application/application.go#L2069-L2165)
- 写入 operation：[SetAppOperation](file:///app/argo-cd/util/argo/argo.go#L938-L958)
- operation 执行入口：[processRequestedAppOperation](file:///app/argo-cd/controller/appcontroller.go#L1430-L1584)
- 预同步比较与执行：[SyncAppState](file:///app/argo-cd/controller/sync.go#L101-L401)
- manifest 获取与 live diff：[CompareAppState](file:///app/argo-cd/controller/state.go#L629-L1189)
- repo-server manifest 生成：[GenerateManifest](file:///app/argo-cd/reposerver/repository/repository.go#L631-L710)
- 最终状态回写：[processAppRefreshQueueItem](file:///app/argo-cd/controller/appcontroller.go#L1691-L1934) + [persistAppStatus](file:///app/argo-cd/controller/appcontroller.go#L2137-L2286)
