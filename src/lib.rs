
#![allow(clippy::missing_safety_doc)]

#[cfg(not(windows))]
compile_error!("This crate currently supports Windows only.");

use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};
use pyo3::Bound;

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::mem::size_of;
use std::os::windows::ffi::OsStrExt;
use std::ptr::{self, null};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_ALREADY_EXISTS, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, MapViewOfFile, OpenFileMappingW, UnmapViewOfFile, FILE_MAP_ALL_ACCESS,
    MEMORY_MAPPED_VIEW_ADDRESS, PAGE_READWRITE,
};

const MAGIC: u64 = 0x5348_4D44_4943_5432; // "SHMDICT2"
const VERSION: u32 = 5; // timeout/recovery header fields

const KEY_MAX: usize = 32;
const VALUE_STR_MAX: usize = 64;

const STATE_EMPTY: u8 = 0;
const STATE_OCCUPIED: u8 = 1;
const STATE_TOMBSTONE: u8 = 2;

const VK_INT: u8 = 1;
const VK_FLOAT: u8 = 2;
const VK_STR_ASCII: u8 = 3;

#[repr(C, align(64))]
struct ShmHeader {
    magic: u64,
    version: u32,
    capacity: u32,
    key_max: u32,
    value_str_max: u32,

    writer: AtomicU32, // 0 free, 1 locked (single writer)
    seq: AtomicU64,    // seqlock: odd=writing, even=stable
    count: AtomicU32,
    _pad1: u32,

    writer_since_unix_ns: AtomicU64, // when current writer lock was acquired
    recovering: AtomicU32,           // 0 free, 1 recovering
    _pad2: u32,
    recover_count: AtomicU64,
}

impl ShmHeader {
    fn new(capacity: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            capacity,
            key_max: KEY_MAX as u32,
            value_str_max: VALUE_STR_MAX as u32,

            writer: AtomicU32::new(0),
            seq: AtomicU64::new(0),
            count: AtomicU32::new(0),
            _pad1: 0,

            writer_since_unix_ns: AtomicU64::new(0),
            recovering: AtomicU32::new(0),
            _pad2: 0,
            recover_count: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ShmValue {
    kind: u8,
    len: u8,
    _pad: [u8; 6],
    payload: [u8; 64],
}

impl ShmValue {
    fn zero() -> Self {
        Self {
            kind: 0,
            len: 0,
            _pad: [0; 6],
            payload: [0; 64],
        }
    }

    fn from_i64(v: i64) -> Self {
        let mut s = Self::zero();
        s.kind = VK_INT;
        s.payload[..8].copy_from_slice(&v.to_le_bytes());
        s
    }

    fn from_f64(v: f64) -> Self {
        let mut s = Self::zero();
        s.kind = VK_FLOAT;
        s.payload[..8].copy_from_slice(&v.to_le_bytes());
        s
    }

    fn from_ascii_str(v: &str) -> PyResult<Self> {
        if !v.is_ascii() {
            return Err(PyValueError::new_err("string value must be ASCII"));
        }
        let b = v.as_bytes();
        if b.len() > VALUE_STR_MAX {
            return Err(PyValueError::new_err(format!(
                "string too long: {} > {}",
                b.len(),
                VALUE_STR_MAX
            )));
        }

        let mut s = Self::zero();
        s.kind = VK_STR_ASCII;
        s.len = b.len() as u8;
        s.payload[..b.len()].copy_from_slice(b);
        Ok(s)
    }
}

#[repr(C, align(64))]
struct ShmEntry {
    state: AtomicU8,
    key_len: u16,
    _pad0: u8,
    _pad1: u32,

    hash: u64,
    value: ShmValue,
    key: [u8; KEY_MAX],
}

struct DictHandle {
    mapping: HANDLE,
    view: MEMORY_MAPPED_VIEW_ADDRESS,
    view_ptr: *mut u8,
    header: *mut ShmHeader,
    entries: *mut ShmEntry,

    spin_ns: u64,
    lock_timeout_ns: u64,
    stale_writer_ns: u64,
    enable_recovery: bool,
}

impl Drop for DictHandle {
    fn drop(&mut self) {
        unsafe {
            if !self.view_ptr.is_null() {
                UnmapViewOfFile(self.view);
            }
            if !self.mapping.is_null() {
                CloseHandle(self.mapping);
            }
        }
    }
}

thread_local! {
    static TLS_HANDLES: RefCell<HashMap<String, DictHandle>> = RefCell::new(HashMap::new());
}

fn now_unix_ns() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as u64,
        Err(_) => 0,
    }
}

fn to_wide_null(s: &str) -> Vec<u16> {
    OsStr::new(s).encode_wide().chain(Some(0)).collect()
}

fn total_size(capacity: u32) -> usize {
    size_of::<ShmHeader>() + capacity as usize * size_of::<ShmEntry>()
}

fn spin_wait_ns(ns: u64) {
    if ns == 0 {
        std::hint::spin_loop();
        return;
    }
    if ns <= 50_000 {
        let start = Instant::now();
        while start.elapsed().as_nanos() < ns as u128 {
            std::hint::spin_loop();
        }
    } else {
        std::thread::sleep(Duration::from_nanos(ns));
    }
}

fn fnv1a64(data: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn is_timeout_err(err: &PyErr) -> bool {
    Python::with_gil(|py| err.is_instance_of::<PyTimeoutError>(py))
}

unsafe fn key_eq(e: &ShmEntry, hash: u64, key: &[u8]) -> bool {
    e.hash == hash && e.key_len as usize == key.len() && &e.key[..key.len()] == key
}

enum SlotResult {
    Found(usize),
    Insert(usize),
    Full,
}

unsafe fn find_slot(entries: *mut ShmEntry, cap: usize, hash: u64, key: &[u8]) -> SlotResult {
    let start = (hash as usize) % cap;
    let mut first_tombstone: Option<usize> = None;

    for i in 0..cap {
        let idx = (start + i) % cap;
        let e = &*entries.add(idx);
        let st = e.state.load(Ordering::Acquire);

        if st == STATE_EMPTY {
            return SlotResult::Insert(first_tombstone.unwrap_or(idx));
        }
        if st == STATE_TOMBSTONE {
            if first_tombstone.is_none() {
                first_tombstone = Some(idx);
            }
            continue;
        }
        if st == STATE_OCCUPIED && key_eq(e, hash, key) {
            return SlotResult::Found(idx);
        }
    }

    if let Some(t) = first_tombstone {
        SlotResult::Insert(t)
    } else {
        SlotResult::Full
    }
}

unsafe fn lookup_once(handle: &DictHandle, key: &[u8], hash: u64) -> Option<ShmValue> {
    let cap = (*handle.header).capacity as usize;
    let start = (hash as usize) % cap;

    for i in 0..cap {
        let idx = (start + i) % cap;
        let e = &*handle.entries.add(idx);
        let st = e.state.load(Ordering::Acquire);

        if st == STATE_EMPTY {
            return None;
        }
        if st == STATE_OCCUPIED && key_eq(e, hash, key) {
            return Some(e.value);
        }
    }
    None
}

// timeout reached时尝试恢复；force=false时仅在stale满足条件才恢复
unsafe fn try_recover(handle: &DictHandle, force: bool) -> bool {
    let h = &*handle.header;

    if h.recovering
        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false;
    }

    let mut recovered = false;
    let now = now_unix_ns();

    let writer = h.writer.load(Ordering::Acquire);
    let seq = h.seq.load(Ordering::Acquire);
    let since = h.writer_since_unix_ns.load(Ordering::Acquire);

    let stale_ok = force
        || (since != 0 && now > since && now - since >= handle.stale_writer_ns)
        || (writer == 0 && (seq & 1) == 1 && since == 0);

    if stale_ok {
        if (seq & 1) == 1 {
            h.seq.fetch_add(1, Ordering::AcqRel); // make even
            recovered = true;
        }
        if writer == 1 {
            h.writer.store(0, Ordering::Release);
            recovered = true;
        }
        if recovered {
            h.writer_since_unix_ns.store(0, Ordering::Release);
            h.recover_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    h.recovering.store(0, Ordering::Release);
    recovered
}

unsafe fn write_begin(handle: &DictHandle, spin_ns: u64, lock_timeout_ns: u64) -> PyResult<()> {
    let start = Instant::now();

    loop {
        if (*handle.header)
            .writer
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            break;
        }

        if start.elapsed().as_nanos() as u64 >= lock_timeout_ns {
            if handle.enable_recovery && try_recover(handle, false) {
                continue;
            }
            return Err(PyTimeoutError::new_err("set timeout waiting for writer lock"));
        }

        spin_wait_ns(spin_ns);
    }

    // normalize odd seq if needed (stale leftover)
    let seq0 = (*handle.header).seq.load(Ordering::Acquire);
    if (seq0 & 1) == 1 {
        (*handle.header).seq.fetch_add(1, Ordering::AcqRel);
    }

    (*handle.header)
        .writer_since_unix_ns
        .store(now_unix_ns(), Ordering::Release);

    (*handle.header).seq.fetch_add(1, Ordering::AcqRel); // odd => writing
    Ok(())
}

unsafe fn write_end(handle: &DictHandle) {
    (*handle.header).seq.fetch_add(1, Ordering::Release); // even => stable
    (*handle.header)
        .writer_since_unix_ns
        .store(0, Ordering::Release);
    (*handle.header).writer.store(0, Ordering::Release);
}

unsafe fn read_with_seq_retry(
    handle: &DictHandle,
    key: &[u8],
    spin_ns: u64,
    lock_timeout_ns: u64,
) -> PyResult<Option<ShmValue>> {
    let hash = fnv1a64(key);
    let start = Instant::now();

    loop {
        let s1 = (*handle.header).seq.load(Ordering::Acquire);
        if (s1 & 1) == 1 {
            if start.elapsed().as_nanos() as u64 >= lock_timeout_ns {
                if handle.enable_recovery && try_recover(handle, false) {
                    continue;
                }
                return Err(PyTimeoutError::new_err("get timeout waiting for stable seq"));
            }
            spin_wait_ns(spin_ns);
            continue;
        }

        let v = lookup_once(handle, key, hash);

        let s2 = (*handle.header).seq.load(Ordering::Acquire);
        if s1 == s2 && (s2 & 1) == 0 {
            return Ok(v);
        }

        if start.elapsed().as_nanos() as u64 >= lock_timeout_ns {
            if handle.enable_recovery && try_recover(handle, false) {
                continue;
            }
            return Err(PyTimeoutError::new_err("get timeout due to repeated seq changes"));
        }

        spin_wait_ns(spin_ns);
    }
}

fn validate_key_bytes(key: &str) -> PyResult<&[u8]> {
    let kb = key.as_bytes();
    if kb.len() > KEY_MAX {
        return Err(PyValueError::new_err(format!(
            "key too long: {} > {} bytes",
            kb.len(),
            KEY_MAX
        )));
    }
    Ok(kb)
}

fn py_to_shm_value(value: &Bound<'_, PyAny>) -> PyResult<ShmValue> {
    if let Ok(v) = value.extract::<i64>() {
        return Ok(ShmValue::from_i64(v));
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(ShmValue::from_f64(v));
    }
    if let Ok(v) = value.extract::<String>() {
        return ShmValue::from_ascii_str(&v);
    }

    Err(PyValueError::new_err(
        "value must be int | float | str(ASCII, len<=64)",
    ))
}

fn shm_value_to_py(py: Python<'_>, v: ShmValue) -> PyResult<PyObject> {
    match v.kind {
        VK_INT => {
            let mut a = [0u8; 8];
            a.copy_from_slice(&v.payload[..8]);
            Ok(i64::from_le_bytes(a).into_py(py))
        }
        VK_FLOAT => {
            let mut a = [0u8; 8];
            a.copy_from_slice(&v.payload[..8]);
            Ok(f64::from_le_bytes(a).into_py(py))
        }
        VK_STR_ASCII => {
            let n = v.len as usize;
            if n > VALUE_STR_MAX {
                return Err(PyRuntimeError::new_err("corrupted string length in shared memory"));
            }
            let s = std::str::from_utf8(&v.payload[..n])
                .map_err(|_| PyRuntimeError::new_err("corrupted non-utf8 bytes in shared memory"))?;
            Ok(s.into_py(py))
        }
        _ => Err(PyRuntimeError::new_err("unknown value kind in shared memory")),
    }
}

fn open_or_create(
    name: &str,
    create: bool,
    capacity: u32,
    spin_ns: u64,
    lock_timeout_ns: u64,
    stale_writer_ns: u64,
    enable_recovery: bool,
) -> PyResult<DictHandle> {
    let cap = if capacity == 0 { 1024 } else { capacity };
    let req_size = total_size(cap);
    let wname = to_wide_null(name);

    unsafe {
        let mapping: HANDLE;
        let mut already_exists = false;

        if create {
            mapping = CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                null(),
                PAGE_READWRITE,
                ((req_size as u64) >> 32) as u32,
                (req_size as u64 & 0xFFFF_FFFF) as u32,
                wname.as_ptr(),
            );
            if mapping.is_null() {
                return Err(PyRuntimeError::new_err("CreateFileMappingW failed"));
            }
            if GetLastError() == ERROR_ALREADY_EXISTS {
                already_exists = true;
            }
        } else {
            mapping = OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, wname.as_ptr());
            if mapping.is_null() {
                return Err(PyRuntimeError::new_err("OpenFileMappingW failed"));
            }
        }

        let view = MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, 0);
        if view.Value.is_null() {
            CloseHandle(mapping);
            return Err(PyRuntimeError::new_err("MapViewOfFile failed"));
        }

        let view_ptr = view.Value as *mut u8;
        let header = view_ptr as *mut ShmHeader;
        let entries = view_ptr.add(size_of::<ShmHeader>()) as *mut ShmEntry;

        if create && !already_exists {
            ptr::write_bytes(view_ptr, 0, req_size);
            ptr::write(header, ShmHeader::new(cap));
        } else {
            if (*header).magic != MAGIC
                || (*header).version != VERSION
                || (*header).key_max != KEY_MAX as u32
                || (*header).value_str_max != VALUE_STR_MAX as u32
            {
                UnmapViewOfFile(view);
                CloseHandle(mapping);
                return Err(PyRuntimeError::new_err(
                    "shared memory layout mismatch (maybe old version mapping still exists)",
                ));
            }
        }

        Ok(DictHandle {
            mapping,
            view,
            view_ptr,
            header,
            entries,
            spin_ns,
            lock_timeout_ns,
            stale_writer_ns,
            enable_recovery,
        })
    }
}

fn open_or_create_auto(
    name: &str,
    capacity: u32,
    spin_ns: u64,
    lock_timeout_ns: u64,
    stale_writer_ns: u64,
    enable_recovery: bool,
) -> PyResult<DictHandle> {
    match open_or_create(
        name,
        false,
        capacity,
        spin_ns,
        lock_timeout_ns,
        stale_writer_ns,
        enable_recovery,
    ) {
        Ok(h) => Ok(h),
        Err(_) => open_or_create(
            name,
            true,
            capacity,
            spin_ns,
            lock_timeout_ns,
            stale_writer_ns,
            enable_recovery,
        ),
    }
}

#[pyclass]
struct SharedMemoryDict {
    name: String,
    spin_ns: u64,
    capacity: u32,
    lock_timeout_ns: u64,
    stale_writer_ns: u64,
    enable_recovery: bool,
    closed: AtomicBool,
}

impl SharedMemoryDict {
    fn with_handle<R>(&self, f: impl FnOnce(&DictHandle) -> PyResult<R>) -> PyResult<R> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err("dict is closed"));
        }

        let name = self.name.clone();
        let spin_ns = self.spin_ns;
        let capacity = self.capacity;
        let lock_timeout_ns = self.lock_timeout_ns;
        let stale_writer_ns = self.stale_writer_ns;
        let enable_recovery = self.enable_recovery;

        TLS_HANDLES.with(|cell| {
            let mut map = cell.borrow_mut();
            if !map.contains_key(&name) {
                let h = open_or_create_auto(
                    &name,
                    capacity,
                    spin_ns,
                    lock_timeout_ns,
                    stale_writer_ns,
                    enable_recovery,
                )?;
                map.insert(name.clone(), h);
            }
            let h = map
                .get(&name)
                .ok_or_else(|| PyRuntimeError::new_err("failed to get thread-local handle"))?;
            f(h)
        })
    }
}

#[pymethods]
impl SharedMemoryDict {
    #[new]
    #[pyo3(signature = (
        name,
        spin_ns=500,
        capacity=1024,
        lock_timeout_us=2_000,
        stale_writer_us=5_000,
        enable_recovery=true
    ))]
    fn new(
        name: &str,
        spin_ns: u64,
        capacity: u32,
        lock_timeout_us: u64,
        stale_writer_us: u64,
        enable_recovery: bool,
    ) -> PyResult<Self> {
        let lock_timeout_ns = lock_timeout_us.saturating_mul(1000);
        let stale_writer_ns = stale_writer_us.saturating_mul(1000);

        let _ = open_or_create_auto(
            name,
            capacity,
            spin_ns,
            lock_timeout_ns,
            stale_writer_ns,
            enable_recovery,
        )?;

        Ok(Self {
            name: name.to_string(),
            spin_ns,
            capacity,
            lock_timeout_ns,
            stale_writer_ns,
            enable_recovery,
            closed: AtomicBool::new(false),
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let name = self.name.clone();
        TLS_HANDLES.with(|cell| {
            cell.borrow_mut().remove(&name);
        });
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// 返回:
    /// - True: 成功
    /// - False: 明显失败（字典满）
    /// - None: 仅并发竞争超时失败
    #[pyo3(signature = (key, value, wait_ns=500, timeout_us=2_000))]
    fn set(
        &self,
        key: &str,
        value: &Bound<'_, PyAny>,
        wait_ns: u64,
        timeout_us: u64,
    ) -> PyResult<Option<bool>> {
        let kb = validate_key_bytes(key)?;
        let hash = fnv1a64(kb);
        let sv = py_to_shm_value(value)?;
        let timeout_ns = timeout_us.saturating_mul(1000);

        self.with_handle(|h| {
            unsafe {
                let cap = (*h.header).capacity as usize;

                if let Err(e) = write_begin(h, wait_ns, timeout_ns) {
                    if is_timeout_err(&e) {
                        return Ok(None);
                    }
                    return Err(e);
                }

                match find_slot(h.entries, cap, hash, kb) {
                    SlotResult::Found(idx) => {
                        let e = &mut *h.entries.add(idx);
                        e.value = sv;
                    }
                    SlotResult::Insert(idx) => {
                        let e = &mut *h.entries.add(idx);
                        e.hash = hash;
                        e.key_len = kb.len() as u16;
                        e.value = sv;
                        e.key[..kb.len()].copy_from_slice(kb);
                        if kb.len() < KEY_MAX {
                            e.key[kb.len()..].fill(0);
                        }
                        e.state.store(STATE_OCCUPIED, Ordering::Release);
                        (*h.header).count.fetch_add(1, Ordering::AcqRel);
                    }
                    SlotResult::Full => {
                        write_end(h);
                        return Ok(Some(false));
                    }
                }

                write_end(h);
            }
            Ok(Some(true))
        })
    }

    /// 行为:
    /// - 找到key: 返回值
    /// - key不存在: 返回default
    /// - 并发竞争超时: 返回None
    #[pyo3(signature = (key, default=None, wait_ns=500, timeout_us=2_000))]
    fn get(
        &self,
        py: Python<'_>,
        key: &str,
        default: Option<PyObject>,
        wait_ns: u64,
        timeout_us: u64,
    ) -> PyResult<PyObject> {
        let kb = validate_key_bytes(key)?;
        let timeout_ns = timeout_us.saturating_mul(1000);

        self.with_handle(|h| unsafe {
            match read_with_seq_retry(h, kb, wait_ns, timeout_ns) {
                Ok(Some(v)) => shm_value_to_py(py, v),
                Ok(None) => Ok(default.unwrap_or_else(|| py.None())),
                Err(e) => {
                    if is_timeout_err(&e) {
                        Ok(py.None())
                    } else {
                        Err(e)
                    }
                }
            }
        })
    }

    fn remove(&self, key: &str) -> PyResult<bool> {
        let kb = validate_key_bytes(key)?;
        let hash = fnv1a64(kb);

        self.with_handle(|h| unsafe {
            let cap = (*h.header).capacity as usize;
            let start = (hash as usize) % cap;

            write_begin(h, h.spin_ns, h.lock_timeout_ns)?;

            let mut found = false;
            for i in 0..cap {
                let idx = (start + i) % cap;
                let e = &mut *h.entries.add(idx);
                let st = e.state.load(Ordering::Acquire);

                if st == STATE_EMPTY {
                    break;
                }
                if st == STATE_OCCUPIED && key_eq(e, hash, kb) {
                    e.state.store(STATE_TOMBSTONE, Ordering::Release);
                    (*h.header).count.fetch_sub(1, Ordering::AcqRel);
                    found = true;
                    break;
                }
            }

            write_end(h);
            Ok(found)
        })
    }

    fn contains(&self, key: &str) -> PyResult<bool> {
        let kb = validate_key_bytes(key)?;
        self.with_handle(|h| unsafe {
            Ok(read_with_seq_retry(h, kb, h.spin_ns, h.lock_timeout_ns)?.is_some())
        })
    }

    fn len(&self) -> PyResult<usize> {
        self.with_handle(|h| unsafe {
            let start = Instant::now();

            loop {
                let s1 = (*h.header).seq.load(Ordering::Acquire);
                if (s1 & 1) == 1 {
                    if start.elapsed().as_nanos() as u64 >= h.lock_timeout_ns {
                        if h.enable_recovery && try_recover(h, false) {
                            continue;
                        }
                        return Err(PyTimeoutError::new_err("len timeout waiting for stable seq"));
                    }
                    spin_wait_ns(h.spin_ns);
                    continue;
                }

                let c = (*h.header).count.load(Ordering::Acquire);

                let s2 = (*h.header).seq.load(Ordering::Acquire);
                if s1 == s2 && (s2 & 1) == 0 {
                    return Ok(c as usize);
                }

                if start.elapsed().as_nanos() as u64 >= h.lock_timeout_ns {
                    if h.enable_recovery && try_recover(h, false) {
                        continue;
                    }
                    return Err(PyTimeoutError::new_err("len timeout due to repeated seq changes"));
                }

                spin_wait_ns(h.spin_ns);
            }
        })
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        self.with_handle(|h| unsafe {
            let start = Instant::now();

            loop {
                let s1 = (*h.header).seq.load(Ordering::Acquire);
                if (s1 & 1) == 1 {
                    if start.elapsed().as_nanos() as u64 >= h.lock_timeout_ns {
                        if h.enable_recovery && try_recover(h, false) {
                            continue;
                        }
                        return Err(PyTimeoutError::new_err("keys timeout waiting for stable seq"));
                    }
                    spin_wait_ns(h.spin_ns);
                    continue;
                }

                let cap = (*h.header).capacity as usize;
                let mut out = Vec::new();

                for i in 0..cap {
                    let e = &*h.entries.add(i);
                    if e.state.load(Ordering::Acquire) == STATE_OCCUPIED {
                        let n = e.key_len as usize;
                        if n <= KEY_MAX {
                            let kb = &e.key[..n];
                            let k = match std::str::from_utf8(kb) {
                                Ok(s) => s.to_string(),
                                Err(_) => String::from_utf8_lossy(kb).to_string(),
                            };
                            out.push(k);
                        }
                    }
                }

                let s2 = (*h.header).seq.load(Ordering::Acquire);
                if s1 == s2 && (s2 & 1) == 0 {
                    return Ok(out);
                }

                if start.elapsed().as_nanos() as u64 >= h.lock_timeout_ns {
                    if h.enable_recovery && try_recover(h, false) {
                        continue;
                    }
                    return Err(PyTimeoutError::new_err("keys timeout due to repeated seq changes"));
                }

                spin_wait_ns(h.spin_ns);
            }
        })
    }

    #[pyo3(signature = (force=false))]
    fn recover(&self, force: bool) -> PyResult<bool> {
        self.with_handle(|h| unsafe { Ok(try_recover(h, force)) })
    }

    fn recovery_count(&self) -> PyResult<u64> {
        self.with_handle(|h| unsafe { Ok((*h.header).recover_count.load(Ordering::Acquire)) })
    }

    fn __len__(&self) -> PyResult<usize> {
        self.len()
    }

    fn __contains__(&self, key: &str) -> PyResult<bool> {
        self.contains(key)
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<PyObject> {
        let v = self.get(py, key, None, self.spin_ns, self.lock_timeout_ns / 1000)?;
        if v.is_none(py) {
            Err(PyKeyError::new_err(key.to_string()))
        } else {
            Ok(v)
        }
    }

    fn __setitem__(&self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        match self.set(key, value, self.spin_ns, self.lock_timeout_ns / 1000)? {
            Some(true) => Ok(()),
            Some(false) => Err(PyRuntimeError::new_err("dictionary is full")),
            None => Err(PyTimeoutError::new_err("__setitem__ timeout due to contention")),
        }
    }

    fn __delitem__(&self, key: &str) -> PyResult<()> {
        if self.remove(key)? {
            Ok(())
        } else {
            Err(PyKeyError::new_err(key.to_string()))
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close();
        Ok(false)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PyObject> {
        let cls = py.get_type_bound::<SharedMemoryDict>();
        let args = (
            self.name.clone(),
            self.spin_ns,
            self.capacity,
            self.lock_timeout_ns / 1000,
            self.stale_writer_ns / 1000,
            self.enable_recovery,
        );
        Ok((cls, args).into_py(py))
    }
}


#[pymodule]
fn shmdict(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SharedMemoryDict>()?;
    Ok(())
}