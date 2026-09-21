## 行动原则
* 每次执行任务前都要查看docs/design_specification/下的约定文件
* 每次完整做完一个编码相关的任务，必须在 *./docs/agent_log* 下生成一个日志md文件，内容至少包括出现了什么问题、做了哪些事情、为什么这么做、结果。每次任务生成一个独立的日志md文件，文件以当前时间和主题、需求命名，并在 *./docs/log_index.md* 中建立索引。
* 若用户不认可当前方案，指出修改方向，则需要及时修改那个日志md文件。
* 每次做一件事情前，在 *./docs/agent_log_index.md* 检索往期是否有可以参考的任务。
* 注意上述的两种文件都需要简洁风格，可读性要强，让用户一下就能抓住重点和细节，知道修改了哪些模块、代码。

## 测试
* 每次测试仅测试当前模块下的内容，不要运行全部测试。


# CodeGraph 代码索引

理解或定位代码时**优先用 CodeGraph，grep/find 其次**。

- 当项目规模达到中型及以上、或者结构复杂时在项目根目录建索引 `.codegraph/`，避免在项目代码路径外创建
- 索引按服务独立，跨服务调用需分别查询
- `wiki/`、`scripts/`、`CLAUDE.md` 等根仓库文件直接 Read/Grep/Glob，不走 CodeGraph
- 代码变更后运行 `codegraph sync <服务目录>` 增量更新；`codegraph init` 由用户决定，Agent 不得自行执行

## 索引必须存放在项目内（强制）

CodeGraph 默认存 `~/.codegraph/`，会跨项目污染。建索引时必须局部覆盖 `HOME`：

```bash
# ✅ 正确：索引存在项目根目录 .codegraph/
HOME="$(pwd)" node codegraph-daemon.js --graph-only -w .

# ❌ 错误：默认存在 ~/.codegraph/，多项目污染
node codegraph-daemon.js --graph-only -w .
```

- `HOME=项目根` 是进程级内联赋值，不影响 git config、SSH keys 等全局配置
- 不要用 `export HOME=...`
- `.codegraph/` 已默认在 `.gitignore` 中

# SonarQube 本地扫描

本机已安装 SonarQube Community Build（本地服务器）+ sonar-scanner-cli（扫描客户端），用于对 Java 项目做静态代码质量/安全分析。

## 安装路径

| 组件 | 路径 |
|---|---|
| SonarQube 服务器 | `C:\Users\35136\JavaProjects\cross-relay-core\tools\sonarqube-25.5.0.107428` |
| sonar-scanner CLI | `C:\Users\35136\JavaProjects\cross-relay-core\tools\sonar-scanner-6.1.0.4477-windows-x64` |

## 使用方法

### 1. 启动本地 SonarQube 服务器

```bash
cmd //c "C:\Users\35136\JavaProjects\cross-relay-core\tools\sonarqube-25.5.0.107428\bin\windows-x86-64\StartSonar.bat"
```

首次启动需要初始化数据库（内置 H2，无需外部 DB），耗时约 1-2 分钟。
启动后访问 `http://localhost:9000`，默认账号 `admin` / `admin`（首次登录强制改密）。

### 2. 获取 token

登录后进入 `My Account → Security → Generate Token`，生成扫描 token。

### 3. 扫描项目

在项目根目录（含 `sonar-project.properties`）执行：

```bash
cmd //c "C:\Users\35136\JavaProjects\cross-relay-core\tools\sonar-scanner-6.1.0.4477-windows-x64\bin\sonar-scanner.bat -Dsonar.host.url=http://localhost:9000 -Dsonar.scanner.skipJreProvisioning=true -Dsonar.login=<TOKEN>"
```

- `-Dsonar.host.url`：必须显式指定，否则默认连 sonarcloud.io
- `-Dsonar.scanner.skipJreProvisioning=true`：本地 Community 服务器不提供 JRE 下载端点（403），必须跳过（自带 JRE 17 运行）

扫描完成后在 `http://localhost:9000/dashboard?id=<projectKey>` 查看报告。

### 4. 停止服务器

```bash
# 找到 StartSonar.bat 启动的 java 进程并终止，或：
taskkill //F //IM java.exe
```

