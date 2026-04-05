# 🚀 shmdict-win

> **Windows 共享内存多进程字典**（`str → int/float/str`）  
> 基于 **Rust + PyO3 + maturin** 构建，极致性能，纳秒级延迟

---

## ✨ 核心特性

| 特性 | 说明 |
|------|------|
| 🔄 **多进程共享** | 同名共享内存，多进程访问同一字典 |
| 📖 **多读单写** | 支持并发读取，单写入者模型 |
| ⚡ **无 OS 锁** | 纯原子操作 + seqlock，用户态自旋 |
| 🎯 **智能重试** | 写/读冲突时纳秒级自旋，持续重试直到成功 |
| 🔑 **Key 限制** | UTF-8 字符串，最大 **32 字节** |
| 💾 **Value 限制** | `i64` 或 UTF-8 字符串，最大 **64 字节** |

---

## 📖 使用说明

### 📦 基本信息

| 项目 | 值 |
|------|-----|
| **包名** | `shmdict` |
| **主类** | `SharedMemoryDict` |
| **平台** | Windows（当前实现仅支持 Windows） |

---

### 1️⃣ 构造函数

```python
SharedMemoryDict(
    name: str,
    spin_ns: int = 500,
    capacity: int = 1024,
    lock_timeout_us: int = 2000,
    stale_writer_us: int = 5000,
    enable_recovery: bool = True
)
```

| 参数 | 类型 | 默认值 | 单位 | 说明 |
|------|------|--------|------|------|
| `name` | `str` | **必填** | — | 共享内存字典名。Windows 推荐 `Global\YourName`，同名即共享同一字典 |
| `spin_ns` | `int` | `500` | ns | 内部默认自旋等待时间，用于 `remove`/`contains`/`len`/`keys` 等方法 |
| `capacity` | `int` | `1024` | entry | 哈希表容量上限（近似键数上限，受 tombstone 影响） |
| `lock_timeout_us` | `int` | `2000` | μs | 内部默认超时，用于未单独传 `timeout` 的方法 |
| `stale_writer_us` | `int` | `5000` | μs | 写锁 stale 判定阈值，启用恢复时超过阈值可能触发恢复 |
| `enable_recovery` | `bool` | `True` | — | 是否允许超时时尝试恢复（修复 odd seq / writer 锁卡死） |

---

### 2️⃣ set（写入操作）⚡

```python
set(
    key: str,
    value: int | float | str,
    wait_ns: int = 500,
    timeout_us: int = 2000
) -> Optional[bool]
```

#### 参数说明

| 参数 | 类型 | 默认值 | 单位 | 说明 |
|------|------|--------|------|------|
| `key` | `str` | **必填** | bytes | UTF-8 编码后长度 ≤ 32 |
| `value` | `int/float/str` | **必填** | — | `str` 必须 ASCII 且长度 ≤ 64 |
| `wait_ns` | `int` | `500` | ns | 并发竞争失败后每轮等待时长 |
| `timeout_us` | `int` | `2000` | μs | 本次调用总超时，超时后不再继续重试 |

#### 返回值语义

| 返回值 | 含义 |
|--------|------|
| `True` | ✅ 写入成功（新插入或覆盖） |
| `False` | ❌ 字典满（明确业务失败，不是竞争） |
| `None` | ⏱️ 仅因并发竞争超时失败（已重试到超时） |

#### ⚠️ 会抛异常的场景

- `key/value` 不合法（长度、类型、ASCII 限制）
- 共享内存打开失败、布局不匹配、对象已关闭等运行时错误

---

### 3️⃣ get（读取操作）📖

```python
get(
    key: str,
    default: Any = None,
    wait_ns: int = 500,
    timeout_us: int = 2000
) -> Any
```

#### 参数说明

| 参数 | 类型 | 默认值 | 单位 | 说明 |
|------|------|--------|------|------|
| `key` | `str` | **必填** | bytes | UTF-8 编码后长度 ≤ 32 |
| `default` | `Any` | `None` | — | key 不存在时返回 |
| `wait_ns` | `int` | `500` | ns | 并发竞争重试轮询等待时长 |
| `timeout_us` | `int` | `2000` | μs | 本次读取总超时 |

#### 返回值语义

| 情况 | 返回值 |
|------|--------|
| 找到 `key` | 返回实际值（`int/float/str`） |
| `key` 不存在 | 返回 `default` |
| 并发竞争超时 | 返回 `None` |

> 💡 **建议**：如果业务值可能为 `None`，请使用"哨兵对象"区分超时与正常结果。

---

### 4️⃣ 其他 API 🔧

| 方法 | 签名 | 说明 |
|------|------|------|
| `remove` | `remove(key: str) -> bool` | 删除成功返回 `True`，不存在返回 `False` |
| `contains` | `contains(key: str) -> bool` | 检查 key 是否存在 |
| `len` | `len() -> int` | 当前计数（并发一致视图） |
| `keys` | `keys() -> list[str]` | 当前 key 快照 |
| `recover` | `recover(force: bool = False) -> bool` | 手动触发恢复（`force=True` 可强制） |
| `recovery_count` | `recovery_count() -> int` | 累计恢复次数（跨进程共享） |
| `close` | `close() -> None` | 关闭当前对象（线程本地句柄清理） |
| `is_closed` | `is_closed() -> bool` | 是否已关闭 |

---

### 5️⃣ 最简示例 💻

```python
from shmdict import SharedMemoryDict

# 创建字典
d = SharedMemoryDict("Global\\DemoDict", capacity=4096, enable_recovery=True)

# 写入数据
ret = d.set("k1", 123, wait_ns=500, timeout_us=50_000)
print("set:", ret)  # True / False / None

# 读取数据
v = d.get("k1", default="MISS", wait_ns=500, timeout_us=50_000)
print("get:", v)

# 关闭连接
d.close()
```

---

## 📊 性能对比测试（RustDict vs UltraDict）

### 汇总表

| Backend | Mode | Total Ops/s | Errors | Recoveries | Reader P99 (μs) | Writer P99 (μs) |
|---------|------|-------------|--------|------------|-----------------|-----------------|
| **Rust** | int | **6,506,758** | 0 | 0 | **1.55** | **1.24** |
| Ultra | int | 6,320,999 | 0 | 0 | 3.09 | 27.52 |
| **Rust** | float | **6,559,147** | 0 | 0 | **1.55** | **1.55** |
| Ultra | float | 6,465,676 | 0 | 0 | 3.09 | 27.21 |
| **Rust** | str | **6,257,108** | 0 | 0 | **1.55** | **1.85** |
| Ultra | str | 6,498,012 | 0 | 0 | 3.09 | 38.34 |
| **Rust** | mixed | **6,321,858** | 0 | 0 | **1.55** | **1.85** |
| Ultra | mixed | 6,409,922 | 0 | 0 | 3.09 | 21.95 |

### 关键差异（Mixed 模式，str_len=64）

| 指标 | RustDict | UltraDict | 优势 |
|------|----------|-----------|------|
| **Total Ops/s** | 6,321,858 | 6,409,922 | Ultra +1.4% |
| **Reader P99** | **1.55 μs** | 3.09 μs | 🏆 **Rust 快 2x** |
| **Reader P999** | **1.85 μs** | 3.40 μs | 🏆 **Rust 快 1.8x** |
| **Writer P99** | **1.85 μs** | 21.95 μs | 🏆 **Rust 快 12x** |
| **Writer P999** | **2.78 μs** | 36.79 μs | 🏆 **Rust 快 13x** |
| **Errors** | 0 | 0 | — |
| **Recoveries** | 0 | 0 | — |

> 🎯 **结论**：RustDict 在**写入延迟**上优势显著（P99 快 12 倍），读取延迟也优于 UltraDict。总吞吐量相当，但 RustDict 尾延迟更低，适合对延迟敏感的场景。

---

## 📝 许可证

MIT License

---

<div align="center">

**Built with ❤️ using Rust + Python**

</div>
