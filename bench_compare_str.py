import argparse
import multiprocessing as mp
import os
import random
import statistics
import time
import uuid
from dataclasses import dataclass
from typing import List, Dict, Any


KEY_MAX = 32
VALUE_STR_MAX = 64




def percentile(sorted_vals: List[int], p: float) -> int:
    if not sorted_vals:
        return 0
    k = int((p / 100.0) * (len(sorted_vals) - 1))
    return sorted_vals[k]


def summarize_lat_ns(lat_ns: List[int]) -> Dict[str, float]:
    if not lat_ns:
        return {
            "count": 0,
            "p50_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "p999_us": 0.0,
            "p9999_us": 0.0,
            "max_us": 0.0,
            "mean_us": 0.0,
        }
    arr = sorted(lat_ns)
    return {
        "count": len(arr),
        "p50_us": percentile(arr, 50.0) / 1000.0,
        "p95_us": percentile(arr, 95.0) / 1000.0,
        "p99_us": percentile(arr, 99.0) / 1000.0,
        "p999_us": percentile(arr, 99.9) / 1000.0,
        "p9999_us": percentile(arr, 99.99) / 1000.0,
        "max_us": arr[-1] / 1000.0,
        "mean_us": statistics.fmean(arr) / 1000.0,
    }


@dataclass
class WorkerResult:
    role: str
    ops: int
    errors: int
    samples: List[int]



class RustAdapter:
    def __init__(
        self,
        name: str,
        is_owner: bool,
        capacity: int,
        spin_ns: int,
        lock_timeout_us: int,
        stale_writer_us: int,
        enable_recovery: bool,
        set_wait_ns: int,
        set_timeout_us: int,
        get_wait_ns: int,
        get_timeout_us: int,
    ):
        from shmdict import SharedMemoryDict

        self.d = SharedMemoryDict(
            name,
            spin_ns=spin_ns,
            capacity=capacity,
            lock_timeout_us=lock_timeout_us,
            stale_writer_us=stale_writer_us,
            enable_recovery=enable_recovery,
        )
        self.set_wait_ns = set_wait_ns
        self.set_timeout_us = set_timeout_us
        self.get_wait_ns = get_wait_ns
        self.get_timeout_us = get_timeout_us

    def set(self, k: str, v: Any) -> bool:
        # Rust: True=成功, False=字典满, None=竞争超时
        ret = self.d.set(
            k, v,
            wait_ns=self.set_wait_ns,
            timeout_us=self.set_timeout_us,
        )
        return ret is True

    def get(self, k: str, default: Any = None):
        # Rust: 超时返回None；不存在返回default
        return self.d.get(
            k, default,
            wait_ns=self.get_wait_ns,
            timeout_us=self.get_timeout_us,
        )

    def recovery_count(self) -> int:
        return int(self.d.recovery_count())

    def close(self):
        self.d.close()


class UltraAdapter:
    def __init__(
        self,
        name: str,
        is_owner: bool,
        capacity: int,
        spin_ns: int,
        lock_timeout_us: int,
        stale_writer_us: int,
        enable_recovery: bool,
        set_wait_ns: int,
        set_timeout_us: int,
        get_wait_ns: int,
        get_timeout_us: int,
    ):
        from UltraDict import UltraDict
        shared_lock = True
        buffer_size = max(8 * 1024 * 1024, capacity * 256)

        if is_owner:
            self.d = UltraDict(
                name=name,
                create=True,
                shared_lock=shared_lock,
                buffer_size=buffer_size,
                auto_unlink=False,
            )
        else:
            last_err = None
            for _ in range(400):
                try:
                    self.d = UltraDict(
                        name=name,
                        create=False,
                        shared_lock=shared_lock,
                        buffer_size=buffer_size,
                        auto_unlink=False,
                    )
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(0.005)
            else:
                raise last_err

    def set(self, k: str, v: Any) -> bool:
        self.d[k] = v
        return True

    def get(self, k: str, default: Any = None):
        try:
            return self.d[k]
        except KeyError:
            return default

    def recovery_count(self) -> int:
        return 0

    def close(self):
        self.d.close()


def get_adapter_cls(backend: str):
    if backend == "rust":
        return RustAdapter
    if backend == "ultra":
        return UltraAdapter
    raise ValueError(f"unknown backend: {backend}")


def make_shm_name(backend: str) -> str:
    uid = uuid.uuid4().hex
    if backend == "rust":
        return f"Global\\BenchRust_{uid}"
    return f"BenchUltra_{uid}"


def gen_ascii_str(op: int, rnd: random.Random, str_len: int) -> str:
    s = (
        f"S{op:016X}"
        f"{rnd.getrandbits(64):016X}"
        f"{rnd.getrandbits(64):016X}"
        f"{rnd.getrandbits(64):016X}"
    )
    return s[:str_len]


def make_value(mode: str, op: int, rnd: random.Random, mix_int: float, mix_float: float, mix_str: float, str_len: int):
    if mode == "int":
        return op
    if mode == "float":
        return op * 0.001 + rnd.random()
    if mode == "str":
        return gen_ascii_str(op, rnd, str_len)
    if mode == "mixed":
        r = rnd.random()
        if r < mix_int:
            return op
        elif r < mix_int + mix_float:
            return op * 0.001 + rnd.random()
        else:
            return gen_ascii_str(op, rnd, str_len)
    raise ValueError(f"unknown mode: {mode}")


def writer_worker(
    backend: str,
    name: str,
    duration_s: float,
    key_count: int,
    sample_stride: int,
    q: mp.Queue,
    capacity: int,
    spin_ns: int,
    lock_timeout_us: int,
    stale_writer_us: int,
    enable_recovery: bool,
    value_mode: str,
    mix_int: float,
    mix_float: float,
    mix_str: float,
    str_len: int,
    set_wait_ns: int,
    set_timeout_us: int,
    get_wait_ns: int,
    get_timeout_us: int,
):
    Adapter = get_adapter_cls(backend)
    d = Adapter(
        name=name, is_owner=False, capacity=capacity, spin_ns=spin_ns,
        lock_timeout_us=lock_timeout_us, stale_writer_us=stale_writer_us, enable_recovery=enable_recovery,
        set_wait_ns=set_wait_ns, set_timeout_us=set_timeout_us, get_wait_ns=get_wait_ns, get_timeout_us=get_timeout_us,
    )

    rnd = random.Random(os.getpid() ^ int(time.time()))
    end_t = time.perf_counter() + duration_s
    ops = 0
    errors = 0
    samples = []
    hot_key = "k0"

    try:
        while time.perf_counter() < end_t:
            k = hot_key if rnd.random() < 0.8 else f"k{rnd.randrange(key_count)}"
            v = make_value(value_mode, ops, rnd, mix_int, mix_float, mix_str, str_len)

            t0 = time.perf_counter_ns()
            try:
                ok = d.set(k, v)
                dt = time.perf_counter_ns() - t0
                if ok:
                    ops += 1
                    if ops % sample_stride == 0:
                        samples.append(dt)
                else:
                    errors += 1
            except Exception:
                errors += 1
    finally:
        d.close()

    q.put(WorkerResult(role="writer", ops=ops, errors=errors, samples=samples))


def reader_worker(
    backend: str,
    name: str,
    duration_s: float,
    key_count: int,
    sample_stride: int,
    q: mp.Queue,
    capacity: int,
    spin_ns: int,
    lock_timeout_us: int,
    stale_writer_us: int,
    enable_recovery: bool,
    value_mode: str,
    set_wait_ns: int,
    set_timeout_us: int,
    get_wait_ns: int,
    get_timeout_us: int,
):
    Adapter = get_adapter_cls(backend)
    d = Adapter(
        name=name, is_owner=False, capacity=capacity, spin_ns=spin_ns,
        lock_timeout_us=lock_timeout_us, stale_writer_us=stale_writer_us, enable_recovery=enable_recovery,
        set_wait_ns=set_wait_ns, set_timeout_us=set_timeout_us, get_wait_ns=get_wait_ns, get_timeout_us=get_timeout_us,
    )

    rnd = random.Random(os.getpid() ^ int(time.time()))
    end_t = time.perf_counter() + duration_s
    ops = 0
    errors = 0
    samples = []
    hot_key = "k0"

    # 用唯一哨兵区分“miss(default)”与“Rust超时返回None”
    sentinel = object()

    try:
        while time.perf_counter() < end_t:
            k = hot_key if rnd.random() < 0.7 else f"k{rnd.randrange(key_count)}"

            t0 = time.perf_counter_ns()
            try:
                v = d.get(k, sentinel)
                dt = time.perf_counter_ns() - t0

                # Rust: 竞争超时 => None
                if v is None:
                    errors += 1
                    continue

                # v is sentinel 表示 key miss，但这是正常路径，不算错误
                ops += 1
                if ops % sample_stride == 0:
                    samples.append(dt)
            except Exception:
                errors += 1
    finally:
        d.close()

    q.put(WorkerResult(role="reader", ops=ops, errors=errors, samples=samples))


def run_once(
    backend: str,
    readers: int,
    duration_s: float,
    key_count: int,
    sample_stride: int,
    capacity: int,
    spin_ns: int,
    lock_timeout_us: int,
    stale_writer_us: int,
    enable_recovery: bool,
    value_mode: str,
    mix_int: float,
    mix_float: float,
    mix_str: float,
    str_len: int,
    set_wait_ns: int,
    set_timeout_us: int,
    get_wait_ns: int,
    get_timeout_us: int,
):
    Adapter = get_adapter_cls(backend)
    shm_name = make_shm_name(backend)

    owner = Adapter(
        name=shm_name, is_owner=True, capacity=capacity, spin_ns=spin_ns,
        lock_timeout_us=lock_timeout_us, stale_writer_us=stale_writer_us, enable_recovery=enable_recovery,
        set_wait_ns=set_wait_ns, set_timeout_us=set_timeout_us, get_wait_ns=get_wait_ns, get_timeout_us=get_timeout_us,
    )
    rnd0 = random.Random(123456)

    for i in range(min(key_count, 10000)):
        ok = owner.set(f"k{i}", make_value(value_mode, i, rnd0, mix_int, mix_float, mix_str, str_len))
        if not ok:
            raise RuntimeError(f"preload failed at k{i}")

    q = mp.Queue()
    procs = []

    p_w = mp.Process(
        target=writer_worker,
        args=(
            backend, shm_name, duration_s, key_count, sample_stride, q,
            capacity, spin_ns, lock_timeout_us, stale_writer_us, enable_recovery,
            value_mode, mix_int, mix_float, mix_str, str_len,
            set_wait_ns, set_timeout_us, get_wait_ns, get_timeout_us
        ),
        daemon=True,
    )
    procs.append(p_w)

    for _ in range(readers):
        p_r = mp.Process(
            target=reader_worker,
            args=(
                backend, shm_name, duration_s, key_count, sample_stride, q,
                capacity, spin_ns, lock_timeout_us, stale_writer_us, enable_recovery,
                value_mode, set_wait_ns, set_timeout_us, get_wait_ns, get_timeout_us
            ),
            daemon=True,
        )
        procs.append(p_r)

    t0 = time.perf_counter()
    results = []
    try:
        for p in procs:
            p.start()
        for _ in procs:
            results.append(q.get())
        for p in procs:
            p.join()
    finally:
        recoveries = owner.recovery_count()
        owner.close()

    elapsed = time.perf_counter() - t0

    writer_ops = sum(r.ops for r in results if r.role == "writer")
    reader_ops = sum(r.ops for r in results if r.role == "reader")
    writer_err = sum(r.errors for r in results if r.role == "writer")
    reader_err = sum(r.errors for r in results if r.role == "reader")

    total_ops = writer_ops + reader_ops
    total_err = writer_err + reader_err

    writer_samples = []
    reader_samples = []
    for r in results:
        if r.role == "writer":
            writer_samples.extend(r.samples)
        else:
            reader_samples.extend(r.samples)

    return {
        "backend": backend,
        "value_mode": value_mode,
        "str_len": str_len,
        "elapsed_s": elapsed,
        "writer_ops_s": writer_ops / elapsed if elapsed > 0 else 0.0,
        "reader_ops_s": reader_ops / elapsed if elapsed > 0 else 0.0,
        "total_ops_s": total_ops / elapsed if elapsed > 0 else 0.0,
        "writer_err": writer_err,
        "reader_err": reader_err,
        "total_err": total_err,
        "recoveries": recoveries,
        "writer_lat": summarize_lat_ns(writer_samples),
        "reader_lat": summarize_lat_ns(reader_samples),
    }


def print_report(rep: Dict[str, Any]):
    print(f"\n=== Backend: {rep['backend']} | mode={rep['value_mode']} | str_len={rep['str_len']} ===")
    print(f"elapsed_s     : {rep['elapsed_s']:.3f}")
    print(f"writer_ops/s  : {rep['writer_ops_s']:.0f}")
    print(f"reader_ops/s  : {rep['reader_ops_s']:.0f}")
    print(f"total_ops/s   : {rep['total_ops_s']:.0f}")
    print(f"errors        : writer={rep['writer_err']} reader={rep['reader_err']} total={rep['total_err']}")
    print(f"recoveries    : {rep['recoveries']}")

    wl = rep["writer_lat"]
    rl = rep["reader_lat"]
    print("\n[Writer latency, us]")
    print(
        f"mean={wl['mean_us']:.2f} p50={wl['p50_us']:.2f} p95={wl['p95_us']:.2f} "
        f"p99={wl['p99_us']:.2f} p999={wl['p999_us']:.2f} p9999={wl['p9999_us']:.2f} max={wl['max_us']:.2f}"
    )
    print("\n[Reader latency, us]")
    print(
        f"mean={rl['mean_us']:.2f} p50={rl['p50_us']:.2f} p95={rl['p95_us']:.2f} "
        f"p99={rl['p99_us']:.2f} p999={rl['p999_us']:.2f} p9999={rl['p9999_us']:.2f} max={rl['max_us']:.2f}"
    )


def print_quick_diff(a: Dict[str, Any], b: Dict[str, Any]):
    print("\n=== Quick Diff (Tail-Focused) ===")
    print(f"mode              : {a['value_mode']} (str_len={a['str_len']})")
    print(f"total_ops/s       : {a['backend']}={a['total_ops_s']:.0f} vs {b['backend']}={b['total_ops_s']:.0f}")
    print(f"reader p99/us     : {a['backend']}={a['reader_lat']['p99_us']:.2f} vs {b['backend']}={b['reader_lat']['p99_us']:.2f}")
    print(f"reader p999/us    : {a['backend']}={a['reader_lat']['p999_us']:.2f} vs {b['backend']}={b['reader_lat']['p999_us']:.2f}")
    print(f"writer p99/us     : {a['backend']}={a['writer_lat']['p99_us']:.2f} vs {b['backend']}={b['writer_lat']['p99_us']:.2f}")
    print(f"writer p999/us    : {a['backend']}={a['writer_lat']['p999_us']:.2f} vs {b['backend']}={b['writer_lat']['p999_us']:.2f}")
    print(f"errors total      : {a['backend']}={a['total_err']} vs {b['backend']}={b['total_err']}")
    print(f"recoveries        : {a['backend']}={a['recoveries']} vs {b['backend']}={b['recoveries']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["rust", "ultra", "both"], default="both")
    parser.add_argument("--readers", type=int, default=8)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--key-count", type=int, default=2000)
    parser.add_argument("--sample-stride", type=int, default=16)
    parser.add_argument("--capacity", type=int, default=65536)
    parser.add_argument("--spin-ns", type=int, default=500)

    parser.add_argument("--lock-timeout-us", type=int, default=200_000)
    parser.add_argument("--stale-writer-us", type=int, default=500_000)
    parser.add_argument("--enable-recovery", action="store_true", default=False)

    # 新增：set/get API 参数（Rust端生效；Ultra忽略）
    parser.add_argument("--set-wait-ns", type=int, default=500)
    parser.add_argument("--set-timeout-us", type=int, default=200_000)
    parser.add_argument("--get-wait-ns", type=int, default=500)
    parser.add_argument("--get-timeout-us", type=int, default=200_000)

    parser.add_argument("--value-modes", type=str, default="int,float,str,mixed")
    parser.add_argument("--mix-int", type=float, default=0.4)
    parser.add_argument("--mix-float", type=float, default=0.3)
    parser.add_argument("--mix-str", type=float, default=0.3)
    parser.add_argument("--str-len", type=int, default=64)
    args = parser.parse_args()

    if not (1 <= args.str_len <= VALUE_STR_MAX):
        raise ValueError(f"--str-len must be in [1, {VALUE_STR_MAX}]")
    if abs((args.mix_int + args.mix_float + args.mix_str) - 1.0) > 1e-9:
        raise ValueError("mixed ratios must sum to 1.0")

    mp.set_start_method("spawn", force=True)

    modes = [m.strip() for m in args.value_modes.split(",") if m.strip()]
    backends = ["rust", "ultra"] if args.backend == "both" else [args.backend]

    all_reports = []

    for mode in modes:
        print(f"\n\n########## Running mode={mode} ##########")
        reps = []
        for b in backends:
            rep = run_once(
                backend=b,
                readers=args.readers,
                duration_s=args.duration,
                key_count=args.key_count,
                sample_stride=args.sample_stride,
                capacity=args.capacity,
                spin_ns=args.spin_ns,
                lock_timeout_us=args.lock_timeout_us,
                stale_writer_us=args.stale_writer_us,
                enable_recovery=args.enable_recovery,
                value_mode=mode,
                mix_int=args.mix_int,
                mix_float=args.mix_float,
                mix_str=args.mix_str,
                str_len=args.str_len,
                set_wait_ns=args.set_wait_ns,
                set_timeout_us=args.set_timeout_us,
                get_wait_ns=args.get_wait_ns,
                get_timeout_us=args.get_timeout_us,
            )
            reps.append(rep)
            all_reports.append(rep)
            print_report(rep)

        if len(reps) == 2:
            print_quick_diff(reps[0], reps[1])

    print("\n\n===== Summary Table =====")
    print("backend | mode  | total_ops/s | errors | recoveries | reader_p99 | writer_p99")
    print("--------|-------|-------------|--------|------------|------------|-----------")
    for r in all_reports:
        print(
            f"{r['backend']:7s} | {r['value_mode']:5s} | {r['total_ops_s']:11.0f} | "
            f"{r['total_err']:6d} | {r['recoveries']:10d} | "
            f"{r['reader_lat']['p99_us']:10.2f} | {r['writer_lat']['p99_us']:9.2f}"
        )





if __name__ == "__main__":
    main()

    # Rust 共享内存字典：
    # key 最大长度为 32 bytes
    # value(str) 最大长度为 64 bytes
    # Python 性能对比脚本：
    # 对比 Rust 字典与 UltraDict
    # 强调吞吐和尾延时（P99/P99.9/P99.99/max）


    # # 1) int/float/str 分开看
    # python bench_compare_str.py --backend both --duration 20 --readers 4 --value-modes int,float,str --str-len 64

    # # 2) mixed 负载
    # python bench_compare_str.py --backend both --duration 20 --readers 4 --value-modes mixed --mix-int 0.4 --mix-float 0.3 --mix-str 0.3 --str-len 64

    # # 3) 高争用
    # python bench_compare_str.py --backend both --duration 30 --readers 8 --key-count 2000 --value-modes int,float,str,mixed --str-len 64


    # # 4) 开启恢复机制
    # python bench_compare_str.py --backend both --duration 30 --readers 8 --key-count 2000  --enable-recovery


    # python包编译步骤:
    # pip install maturin
    # cargo clean
    # maturin init
    # maturin build --release

    # # 再从 wheel 安装（推荐方式）
    # pip install --force-reinstall shmdict_win-0.1.0-cp38-abi3-win_amd64.whl

