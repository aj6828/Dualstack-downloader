"""
Dual-Stack Download Accelerator — single-file build.

Fixes applied from code review:
 #1  resolve() results are actually used: A and AAAA resolved once, a chosen
     IP is passed into each worker, connections go to that exact IP (no
     re-resolution inside connect()).
 #2  Ranged GETs now REQUIRE HTTP 206 and validate Content-Range. A 200 on
     a ranged request is a hard error (server ignored Range).
 #3  Chunk requeue / worker lifecycle cleaned up. Workers stay alive after
     a chunk failure (unless the whole download is stopping); re-queued
     chunks simply go back on the deque. No task_done() reliance.
 #4  Per-worker file handles. Each worker opens its own "r+b" handle and
     seeks independently. No shared write lock.
 #5  Cross-family identity check. Probe runs against IPv4 AND IPv6 (when
     both resolve), and Content-Length / ETag / Last-Modified must match.
     If they disagree, dual-stack is refused and only one family is used.
 #6  Rolling EMA speed per worker plus total-per-family aggregation in
     the scheduler. No more bursty per-chunk resets.
 #7  JSON sidecar resume (.dsdl next to the output file).
 #8  Probe tries HEAD first, falls back to a one-byte ranged GET (bytes=0-0)
     if HEAD fails or is unhelpful.
 #9  Worker counts and all other tunables exposed in the Options dialog.
 #10 Pause / Resume / Cancel in the GUI.

Run:      python dualstack_dl.py
Package:  pyinstaller --onefile --windowed dualstack_dl.py
"""

import hashlib
import http.client
import json
import http.cookies
import os
import socket
import ssl
import tempfile
import threading
import time
import tkinter as tk
import urllib.parse
from collections import deque
from pathlib import Path
from tkinter import ttk, filedialog, messagebox


# ====================================================================
# CONFIG
# ====================================================================

APP_DIR = Path.home() / ".dualstack_dl"
APP_DIR.mkdir(exist_ok=True)
SETTINGS_FILE = APP_DIR / "settings.json"

DEFAULTS = {
    "ipv6_workers": 4,
    "ipv4_workers": 2,
    "chunk_size_mb": 8,
    "max_retries": 5,
    "connect_timeout": 10,
    "read_timeout": 30,
    "stall_threshold_kbps": 50,
    "stall_window_sec": 8,
    "speed_limit_kbps": 0,
    "user_agent": "dualstack-dl/1.1",
    "custom_headers": {},
    "output_folder": str(Path.home() / "Downloads"),
    "auto_shutdown": False,
    "max_concurrent_downloads": 2,
    "speed_probe_seconds": 2,
    "speed_probe_enabled": True,
    "keepalive_enabled": True,
    "ema_alpha": 0.3,
    "insecure_ssl": False,
    "allow_single_stream_fallback": True,
    "max_redirects": 6,
}


def load_settings():
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                merged = dict(DEFAULTS)
                merged.update(json.load(f))
                return merged
        except Exception:
            pass
    return dict(DEFAULTS)


def save_settings(s):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2)
    except Exception as e:
        print(f"settings save failed: {e}")


# ====================================================================
# DUAL-STACK RESOLVER + FAMILY-PINNED CONNECTIONS
# ====================================================================

def resolve(host):
    """Resolve once. Return (a_records, aaaa_records)."""
    a, aaaa = [], []
    try:
        for info in socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM):
            ip = info[4][0]
            if ip not in a:
                a.append(ip)
    except socket.gaierror:
        pass
    try:
        for info in socket.getaddrinfo(host, None, socket.AF_INET6, socket.SOCK_STREAM):
            ip = info[4][0]
            if ip not in aaaa:
                aaaa.append(ip)
    except socket.gaierror:
        pass
    return a, aaaa


class DirectHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection to a specific IP, SNI=original hostname."""

    def __init__(self, host, port, ip, family, connect_timeout, read_timeout, insecure=False):
        super().__init__(host, port=port)
        self._ip = ip
        self._family = family
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._insecure = insecure

    def connect(self):
        sock = socket.socket(self._family, socket.SOCK_STREAM)
        sock.settimeout(self._connect_timeout)
        if self._family == socket.AF_INET6:
            sock.connect((self._ip, self.port, 0, 0))
        else:
            sock.connect((self._ip, self.port))
        sock.settimeout(self._read_timeout)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            pass
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        if self._insecure:
            ctx = ssl._create_unverified_context()
            self.sock = ctx.wrap_socket(sock, server_hostname=self.host)
        else:
            ctx = ssl.create_default_context()
            self.sock = ctx.wrap_socket(sock, server_hostname=self.host)


class DirectHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host, port, ip, family, connect_timeout, read_timeout):
        super().__init__(host, port=port)
        self._ip = ip
        self._family = family
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout

    def connect(self):
        sock = socket.socket(self._family, socket.SOCK_STREAM)
        sock.settimeout(self._connect_timeout)
        if self._family == socket.AF_INET6:
            sock.connect((self._ip, self.port, 0, 0))
        else:
            sock.connect((self._ip, self.port))
        sock.settimeout(self._read_timeout)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            pass
        self.sock = sock


def make_direct_conn(url, ip, family, connect_timeout, read_timeout, insecure_ssl=False):
    p = urllib.parse.urlparse(url)
    host = p.hostname
    port = p.port or (443 if p.scheme == "https" else 80)
    cls = DirectHTTPSConnection if p.scheme == "https" else DirectHTTPConnection
    if cls is DirectHTTPSConnection:
        return cls(host, port, ip, family, connect_timeout, read_timeout, insecure=insecure_ssl)
    return cls(host, port, ip, family, connect_timeout, read_timeout)


def url_path(url):
    p = urllib.parse.urlparse(url)
    path = p.path or "/"
    if p.query:
        path += "?" + p.query
    return path


# ====================================================================
# BROWSER-LIKE HELPERS
# ====================================================================

def resolve_first_ip(host, family):
    recs = resolve(host)
    ips = recs[1] if family == socket.AF_INET6 else recs[0]
    return ips[0] if ips else None


class SimpleCookieJar:
    def __init__(self):
        self._cookies = {}
        self._lock = threading.Lock()

    def add_from_response(self, host, headers):
        vals = []
        try:
            vals = headers.get_all("Set-Cookie") or []
        except Exception:
            one = headers.get("Set-Cookie")
            if one:
                vals = [one]
        if not vals:
            return
        with self._lock:
            jar = self._cookies.setdefault(host, {})
            for raw in vals:
                try:
                    c = http.cookies.SimpleCookie()
                    c.load(raw)
                    for morsel in c.values():
                        jar[morsel.key] = morsel.value
                except Exception:
                    continue

    def header_for(self, host):
        with self._lock:
            jar = self._cookies.get(host, {})
            if not jar:
                return None
            return "; ".join(f"{k}={v}" for k, v in jar.items())


def request_follow(url, family, user_agent, extra_headers, ct, rt, method="GET", range_header=None, cookie_jar=None, max_redirects=6, insecure_ssl=False):
    current_url = url
    body = None
    status = None
    last_headers = None
    for _ in range(max_redirects + 1):
        p = urllib.parse.urlparse(current_url)
        if not p.hostname:
            raise RuntimeError("invalid redirect URL")
        ip = resolve_first_ip(p.hostname, family)
        if not ip:
            raise RuntimeError(f"DNS failed for {p.hostname}")
        headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
        headers.update(extra_headers or {})
        if range_header:
            headers["Range"] = range_header
        if cookie_jar is not None:
            ck = cookie_jar.header_for(p.hostname)
            if ck:
                headers["Cookie"] = ck
        conn = make_direct_conn(current_url, ip, family, ct, rt, insecure_ssl=insecure_ssl)
        conn.request(method, url_path(current_url), headers=headers)
        resp = conn.getresponse()
        if cookie_jar is not None:
            cookie_jar.add_from_response(p.hostname, resp.headers)
        status = resp.status
        last_headers = resp.headers
        if status in (301, 302, 303, 307, 308):
            loc = resp.getheader("Location")
            resp.read()
            conn.close()
            if not loc:
                raise RuntimeError("redirect without Location")
            current_url = urllib.parse.urljoin(current_url, loc)
            continue
        body = resp
        return current_url, ip, body, last_headers
    raise RuntimeError("too many redirects")


# ====================================================================
# PROBE  (HEAD, fallback to GET Range: bytes=0-0)
# ====================================================================

def _probe_via(url, ip, family, user_agent, extra_headers, ct, rt, cookie_jar=None, insecure_ssl=False, allow_single_stream_fallback=True, max_redirects=6):
    """Probe one family. Follows redirects with re-resolution and carries cookies."""
    size = None
    ranges = False
    etag = None
    last_mod = None
    final_url = url
    final_ip = ip
    disposition = None
    content_type = None

    try:
        final_url, final_ip, r, hdrs = request_follow(
            url, family, user_agent, extra_headers, ct, rt,
            method="HEAD", cookie_jar=cookie_jar, max_redirects=max_redirects, insecure_ssl=insecure_ssl,
        )
        if r.status < 400:
            cl = r.getheader("Content-Length")
            if cl and cl.isdigit():
                size = int(cl)
            ranges = r.getheader("Accept-Ranges", "").lower() == "bytes"
            etag = r.getheader("ETag")
            last_mod = r.getheader("Last-Modified")
            disposition = r.getheader("Content-Disposition")
            content_type = r.getheader("Content-Type")
        r.read()
        r.close()
    except Exception:
        pass

    if size is None or not ranges:
        try:
            final_url, final_ip, r, hdrs = request_follow(
                final_url, family, user_agent, extra_headers, ct, rt,
                method="GET", range_header="bytes=0-0", cookie_jar=cookie_jar, max_redirects=max_redirects, insecure_ssl=insecure_ssl,
            )
            disposition = disposition or r.getheader("Content-Disposition")
            content_type = content_type or r.getheader("Content-Type")
            if r.status == 206:
                cr = r.getheader("Content-Range", "")
                if "/" in cr:
                    total = cr.rsplit("/", 1)[-1].strip()
                    if total.isdigit():
                        size = int(total)
                        ranges = True
                etag = etag or r.getheader("ETag")
                last_mod = last_mod or r.getheader("Last-Modified")
            elif r.status == 200:
                cl = r.getheader("Content-Length")
                if cl and cl.isdigit():
                    size = int(cl)
                etag = etag or r.getheader("ETag")
                last_mod = last_mod or r.getheader("Last-Modified")
            r.read()
            r.close()
        except Exception:
            pass

    single_ok = False
    if allow_single_stream_fallback and size is not None and not ranges:
        ct_hdr = (content_type or "").lower()
        if disposition or (ct_hdr and not ct_hdr.startswith("text/html")):
            single_ok = True

    if size is not None and (ranges or single_ok):
        return {
            "final_url": final_url,
            "final_ip": final_ip,
            "size": size,
            "ranges": ranges,
            "single_stream": single_ok and not ranges,
            "etag": etag,
            "last_modified": last_mod,
            "content_disposition": disposition,
            "content_type": content_type,
        }
    return None


def probe_dual(url, user_agent, extra_headers, ct, rt, cookie_jar=None, insecure_ssl=False, allow_single_stream_fallback=True, max_redirects=6):
    """Resolve both families, probe both, verify they describe the same object."""
    p = urllib.parse.urlparse(url)
    if not p.hostname:
        raise RuntimeError("invalid URL")

    a_list, aaaa_list = resolve(p.hostname)
    if not a_list and not aaaa_list:
        raise RuntimeError(f"DNS failed for {p.hostname}")

    v6_probe = None
    for ip in aaaa_list:
        v6_probe = _probe_via(url, ip, socket.AF_INET6, user_agent, extra_headers, ct, rt, cookie_jar=cookie_jar, insecure_ssl=insecure_ssl, allow_single_stream_fallback=allow_single_stream_fallback, max_redirects=max_redirects)
        if v6_probe:
            v6_probe["ip"] = v6_probe.get("final_ip") or ip
            break

    v4_probe = None
    for ip in a_list:
        v4_probe = _probe_via(url, ip, socket.AF_INET, user_agent, extra_headers, ct, rt, cookie_jar=cookie_jar, insecure_ssl=insecure_ssl, allow_single_stream_fallback=allow_single_stream_fallback, max_redirects=max_redirects)
        if v4_probe:
            v4_probe["ip"] = v4_probe.get("final_ip") or ip
            break

    if not v4_probe and not v6_probe:
        raise RuntimeError("probe failed on both IPv4 and IPv6")

    dual = False
    if v4_probe and v6_probe:
        if v4_probe["size"] != v6_probe["size"]:
            dual = False
        elif bool(v4_probe.get("ranges")) != bool(v6_probe.get("ranges")):
            dual = False
        elif v4_probe.get("etag") and v6_probe.get("etag"):
            dual = v4_probe["etag"] == v6_probe["etag"]
        elif v4_probe.get("last_modified") and v6_probe.get("last_modified"):
            dual = v4_probe["last_modified"] == v6_probe["last_modified"]
        else:
            dual = True

    chosen = v6_probe or v4_probe
    if v4_probe and v6_probe and not dual:
        chosen = v6_probe
        v4_probe = None

    if chosen["size"] <= 0:
        raise RuntimeError("server did not report Content-Length")

    return {
        "final_url": chosen["final_url"],
        "size": chosen["size"],
        "etag": chosen.get("etag"),
        "last_modified": chosen.get("last_modified"),
        "ipv4_ip": v4_probe["ip"] if v4_probe else None,
        "ipv6_ip": v6_probe["ip"] if v6_probe and (dual or not v4_probe) else (chosen["ip"] if chosen is v6_probe else None),
        "dual_stack": bool(v4_probe and v6_probe and dual and chosen.get("ranges")),
        "supports_ranges": bool(chosen.get("ranges")),
        "single_stream": bool(chosen.get("single_stream")),
    }


def speed_probe(url, ip, family, user_agent, extra_headers, duration, ct, rt, insecure_ssl=False):
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "identity",
        "Range": "bytes=0-4194303",
    }
    headers.update(extra_headers or {})
    try:
        conn = make_direct_conn(url, ip, family, ct, rt, insecure_ssl=insecure_ssl)
        conn.request("GET", url_path(url), headers=headers)
        r = conn.getresponse()
        if r.status != 206:
            r.read()
            conn.close()
            return 0.0
        t0 = time.monotonic()
        deadline = t0 + duration
        got = 0
        while time.monotonic() < deadline:
            buf = r.read(65536)
            if not buf:
                break
            got += len(buf)
        elapsed = time.monotonic() - t0
        conn.close()
        return got / elapsed if elapsed > 0 else 0.0
    except Exception:
        return 0.0


# ====================================================================
# RESUME SIDECAR
# ====================================================================

def sidecar_path(output_path):
    return str(output_path) + ".dsdl"


def sidecar_load(output_path):
    p = sidecar_path(output_path)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def sidecar_save(output_path, state):
    p = sidecar_path(output_path)
    d = os.path.dirname(p) or "."
    fd, tmp = tempfile.mkstemp(prefix=".dsdl-", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, p)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def sidecar_delete(output_path):
    try:
        os.unlink(sidecar_path(output_path))
    except FileNotFoundError:
        pass


def new_chunk_list(size, chunk_size):
    out = []
    start = 0
    idx = 0
    while start < size:
        end = min(start + chunk_size - 1, size - 1)
        out.append({
            "idx": idx, "start": start, "end": end,
            "done": False, "bytes_written": 0,
            "family": None, "retries": 0,
        })
        start = end + 1
        idx += 1
    return out


def sidecar_valid_against(state, probe_result):
    if state["size"] != probe_result["size"]:
        return False
    if state.get("etag") and probe_result.get("etag"):
        return state["etag"] == probe_result["etag"]
    if state.get("last_modified") and probe_result.get("last_modified"):
        return state["last_modified"] == probe_result["last_modified"]
    return True


# ====================================================================
# TOKEN BUCKET
# ====================================================================

class TokenBucket:
    def __init__(self, rate_bps):
        self.rate = rate_bps
        self.tokens = rate_bps
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def set_rate(self, rate_bps):
        with self.lock:
            self.rate = rate_bps

    def consume(self, n):
        if self.rate <= 0:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                if self.tokens >= n:
                    self.tokens -= n
                    return
                need = n - self.tokens
                wait = need / self.rate
            time.sleep(min(wait, 0.1))


# ====================================================================
# WORKER
# ====================================================================

class Worker(threading.Thread):
    """One thread, one family, one resolved IP, one private file handle."""

    def __init__(self, name, family, ip, scheduler, settings, bucket):
        super().__init__(daemon=True)
        self.name = name
        self.family = family
        self.family_tag = "v6" if family == socket.AF_INET6 else "v4"
        self.ip = ip
        self.sched = scheduler
        self.settings = settings
        self.bucket = bucket

        self.bytes_total = 0
        self.current_speed = 0.0  # EMA bytes/sec
        self.state = "idle"
        self.last_error = None
        self.active_chunk = None

        self._conn = None
        self._fh = None
        self._ema_alpha = float(settings.get("ema_alpha", 0.3))
        self._last_sample_time = None
        self._sample_bytes = 0

    def _get_conn(self):
        if not self.settings["keepalive_enabled"]:
            self._close_conn()
        if self._conn is None:
            self._conn = make_direct_conn(
                self.sched.url, self.ip, self.family,
                self.settings["connect_timeout"],
                self.settings["read_timeout"],
                insecure_ssl=self.settings.get("insecure_ssl", False),
            )
        return self._conn

    def _close_conn(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _update_speed(self, nbytes):
        self._sample_bytes += nbytes
        now = time.monotonic()
        if self._last_sample_time is None:
            self._last_sample_time = now
            return
        dt = now - self._last_sample_time
        if dt >= 0.5:
            instant = self._sample_bytes / dt
            if self.current_speed == 0:
                self.current_speed = instant
            else:
                a = self._ema_alpha
                self.current_speed = a * instant + (1 - a) * self.current_speed
            self._last_sample_time = now
            self._sample_bytes = 0

    def run(self):
        self.state = "running"
        try:
            self._fh = open(self.sched.output_path, "r+b", buffering=0)
        except Exception as e:
            self.state = "error"
            self.last_error = f"open failed: {e}"
            return

        try:
            while True:
                if self.sched.should_stop():
                    break
                self.sched.wait_if_paused()

                chunk = self.sched.claim_chunk()
                if chunk is None:
                    break

                self.active_chunk = chunk
                ok = self._fetch_chunk(chunk)
                self.active_chunk = None

                if ok:
                    chunk["family"] = self.family_tag
                    self.sched.complete_chunk(chunk)
                else:
                    self.sched.requeue_chunk(chunk)
                    self._close_conn()
                    time.sleep(0.2)
        finally:
            self._close_conn()
            if self._fh is not None:
                try:
                    self._fh.close()
                except Exception:
                    pass
                self._fh = None
            self.current_speed = 0.0
            if self.state != "error":
                self.state = "done"

    def _fetch_chunk(self, chunk):
        path = url_path(self.sched.url)
        base_headers = {
            "User-Agent": self.settings["user_agent"],
            "Accept-Encoding": "identity",
            "Connection": "keep-alive" if self.settings["keepalive_enabled"] else "close",
        }
        base_headers.update(self.settings.get("custom_headers") or {})
        ck = self.sched.cookie_jar.header_for(urllib.parse.urlparse(self.sched.url).hostname)
        if ck:
            base_headers["Cookie"] = ck

        max_retries = self.settings["max_retries"]
        window_bytes = 0
        window_start = time.monotonic()
        stall_kbps = self.settings["stall_threshold_kbps"]
        stall_window = self.settings["stall_window_sec"]

        for attempt in range(max_retries):
            if self.sched.should_stop():
                return False
            self.sched.wait_if_paused()

            start = chunk["start"] + chunk["bytes_written"]
            end = chunk["end"]
            if start > end:
                return True
            expected_remaining = end - start + 1

            headers = dict(base_headers)
            headers["Range"] = f"bytes={start}-{end}"

            try:
                conn = self._get_conn()
                if not self.sched.supports_ranges:
                    headers.pop("Range", None)
                conn.request("GET", path, headers=headers)
                resp = conn.getresponse()
                self.sched.cookie_jar.add_from_response(urllib.parse.urlparse(self.sched.url).hostname, resp.headers)

                # FIX #2: 200 on a ranged request = server ignored Range
                if self.sched.supports_ranges and resp.status == 200:
                    resp.read()
                    self._close_conn()
                    self.last_error = "server ignored Range (got 200)"
                    self.state = "error"
                    return False

                if resp.status == 429 or resp.status >= 500:
                    retry_after = resp.getheader("Retry-After")
                    resp.read()
                    self._close_conn()
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else min(30, 2 ** attempt)
                    chunk["retries"] += 1
                    time.sleep(wait)
                    continue

                if self.sched.supports_ranges and resp.status != 206:
                    resp.read()
                    self._close_conn()
                    self.last_error = f"HTTP {resp.status}"
                    chunk["retries"] += 1
                    time.sleep(min(10, 2 ** attempt))
                    continue

                # Validate Content-Range matches what we asked for
                cr = resp.getheader("Content-Range", "")
                if self.sched.supports_ranges and cr:
                    try:
                        spec = cr.split()[1]  # "START-END/TOTAL"
                        rng, _total = spec.split("/")
                        got_start, got_end = [int(x) for x in rng.split("-")]
                        if got_start != start or got_end != end:
                            resp.read()
                            self._close_conn()
                            self.last_error = f"bad Content-Range: {cr} (wanted {start}-{end})"
                            chunk["retries"] += 1
                            time.sleep(1)
                            continue
                    except Exception:
                        resp.read()
                        self._close_conn()
                        self.last_error = f"unparsable Content-Range: {cr}"
                        chunk["retries"] += 1
                        continue

                # Stream
                offset = start
                got = 0
                self._last_sample_time = time.monotonic()

                while got < expected_remaining:
                    if self.sched.should_stop():
                        self._close_conn()
                        return False
                    self.sched.wait_if_paused()

                    buf = resp.read(65536)
                    if not buf:
                        break

                    self.bucket.consume(len(buf))

                    # Per-worker write — no shared lock
                    self._fh.seek(offset + got)
                    self._fh.write(buf)

                    got += len(buf)
                    chunk["bytes_written"] += len(buf)
                    self.bytes_total += len(buf)
                    window_bytes += len(buf)
                    self._update_speed(len(buf))

                    now = time.monotonic()
                    if now - window_start >= stall_window:
                        kbps = (window_bytes / (now - window_start)) / 1024
                        if kbps < stall_kbps:
                            self.last_error = f"stalled @ {kbps:.1f} KB/s"
                            self._close_conn()
                            chunk["retries"] += 1
                            break
                        window_start = now
                        window_bytes = 0

                self.sched.maybe_persist()

                if chunk["bytes_written"] >= (chunk["end"] - chunk["start"] + 1):
                    return True

                # short read — retry remaining
                chunk["retries"] += 1
                time.sleep(0.3)
                continue

            except Exception as e:
                self.last_error = str(e)
                self._close_conn()
                chunk["retries"] += 1
                time.sleep(min(10, 2 ** attempt))
                continue

        return False


# ====================================================================
# SCHEDULER
# ====================================================================

class Download:
    def __init__(self, url, output_path, settings, sha256=None):
        self.url = url
        self.output_path = output_path
        self.settings = dict(settings)
        self.sha256 = (sha256 or "").strip().lower() or None

        self.state = "pending"
        self.error = None
        self.size = 0
        self.final_url = url
        self.etag = None
        self.last_modified = None
        self.ipv4_ip = None
        self.ipv6_ip = None
        self.dual_stack = False
        self.supports_ranges = True
        self.single_stream = False
        self.cookie_jar = SimpleCookieJar()

        self._chunks = []
        self._pending = deque()
        self._in_flight = set()
        self._lock = threading.Lock()

        self._pause_event = threading.Event()
        self._pause_event.set()
        self._stop_event = threading.Event()

        self._bucket = TokenBucket(self.settings["speed_limit_kbps"] * 1024)

        self._workers = []
        self._thread = None
        self._started_at = None
        self._last_persist = 0.0
        self.listeners = []

    def start(self):
        if self.state in ("running", "probing"):
            return
        self._stop_event.clear()
        self._pause_event.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def pause(self):
        if self.state == "running":
            self._pause_event.clear()
            self.state = "paused"

    def resume(self):
        if self.state == "paused":
            self._pause_event.set()
            self.state = "running"

    def cancel(self):
        self._stop_event.set()
        self._pause_event.set()
        self.state = "cancelled"

    def set_speed_limit_kbps(self, kbps):
        self.settings["speed_limit_kbps"] = kbps
        self._bucket.set_rate(kbps * 1024)

    def should_stop(self):
        return self._stop_event.is_set()

    def wait_if_paused(self):
        self._pause_event.wait()

    def claim_chunk(self):
        with self._lock:
            while self._pending:
                c = self._pending.popleft()
                if c["done"]:
                    continue
                self._in_flight.add(c["idx"])
                return c
            return None

    def complete_chunk(self, chunk):
        with self._lock:
            chunk["done"] = True
            self._in_flight.discard(chunk["idx"])
        self._persist()

    def requeue_chunk(self, chunk):
        with self._lock:
            self._in_flight.discard(chunk["idx"])
            if not chunk["done"]:
                self._pending.append(chunk)

    def maybe_persist(self):
        now = time.monotonic()
        if now - self._last_persist > 2.0:
            self._last_persist = now
            self._persist()

    @property
    def bytes_done(self):
        with self._lock:
            return sum(c["bytes_written"] for c in self._chunks)

    @property
    def overall_speed(self):
        if not self._started_at:
            return 0.0
        elapsed = time.monotonic() - self._started_at
        return self.bytes_done / elapsed if elapsed > 0 else 0.0

    @property
    def speed_v6(self):
        return sum(w.current_speed for w in self._workers if w.family_tag == "v6")

    @property
    def speed_v4(self):
        return sum(w.current_speed for w in self._workers if w.family_tag == "v4")

    @property
    def progress_pct(self):
        return 100 * self.bytes_done / self.size if self.size else 0.0

    @property
    def eta_seconds(self):
        sp = self.overall_speed
        return (self.size - self.bytes_done) / sp if sp > 0 else -1

    def worker_stats(self):
        return [
            {
                "name": w.name, "family": w.family_tag, "ip": w.ip,
                "speed": w.current_speed, "bytes": w.bytes_total,
                "state": w.state, "error": w.last_error,
                "active_chunk": w.active_chunk["idx"] if w.active_chunk else None,
            }
            for w in self._workers
        ]

    def _run(self):
        try:
            self.state = "probing"

            pr = probe_dual(
                self.url,
                self.settings["user_agent"],
                self.settings.get("custom_headers"),
                self.settings["connect_timeout"],
                self.settings["read_timeout"],
                cookie_jar=self.cookie_jar,
                insecure_ssl=self.settings.get("insecure_ssl", False),
                allow_single_stream_fallback=self.settings.get("allow_single_stream_fallback", True),
                max_redirects=self.settings.get("max_redirects", 6),
            )

            self.size = pr["size"]
            self.final_url = pr["final_url"]
            self.url = pr["final_url"]
            self.etag = pr["etag"]
            self.last_modified = pr["last_modified"]
            self.ipv4_ip = pr["ipv4_ip"]
            self.ipv6_ip = pr["ipv6_ip"]
            self.dual_stack = pr["dual_stack"]
            self.supports_ranges = pr.get("supports_ranges", True)
            self.single_stream = pr.get("single_stream", False)

            # Resume or fresh
            existing = sidecar_load(self.output_path)
            chunk_size = self.settings["chunk_size_mb"] * 1024 * 1024

            fresh = True
            if existing and sidecar_valid_against(existing, pr):
                self._chunks = existing["chunks"]
                fresh = False
            else:
                if existing:
                    try:
                        os.unlink(self.output_path)
                    except FileNotFoundError:
                        pass
                self._chunks = ([{"idx": 0, "start": 0, "end": self.size - 1, "done": False, "bytes_written": 0, "family": None, "retries": 0}] if not self.supports_ranges else new_chunk_list(self.size, chunk_size))

            if not self.supports_ranges and existing and not fresh:
                # Single-stream fallback cannot safely resume mid-file without ranges.
                try:
                    os.unlink(self.output_path)
                except FileNotFoundError:
                    pass
                self._chunks = [{"idx": 0, "start": 0, "end": self.size - 1, "done": False, "bytes_written": 0, "family": None, "retries": 0}]
                fresh = True

            # Pre-allocate
            if not os.path.exists(self.output_path):
                with open(self.output_path, "wb") as f:
                    f.truncate(self.size)
            elif os.path.getsize(self.output_path) != self.size:
                with open(self.output_path, "r+b") as f:
                    f.truncate(self.size)

            for c in self._chunks:
                if not c["done"]:
                    self._pending.append(c)

            if not self._pending:
                self._finish()
                return

            if fresh and self.supports_ranges and self.settings["speed_probe_enabled"] and self.ipv4_ip and self.ipv6_ip:
                self._auto_balance()

            self._started_at = time.monotonic()
            self.state = "running"

            if self.supports_ranges:
                v6n = self.settings["ipv6_workers"] if self.ipv6_ip else 0
                v4n = self.settings["ipv4_workers"] if self.ipv4_ip else 0
            else:
                v6n = 1 if self.ipv6_ip else 0
                v4n = 0 if self.ipv6_ip else (1 if self.ipv4_ip else 0)
            if v6n + v4n == 0:
                raise RuntimeError("no usable address family")

            for i in range(v6n):
                self._workers.append(Worker(
                    f"v6-{i+1}", socket.AF_INET6, self.ipv6_ip,
                    self, self.settings, self._bucket,
                ))
            for i in range(v4n):
                self._workers.append(Worker(
                    f"v4-{i+1}", socket.AF_INET, self.ipv4_ip,
                    self, self.settings, self._bucket,
                ))

            for w in self._workers:
                w.start()

            while any(w.is_alive() for w in self._workers):
                if self._stop_event.is_set():
                    break
                time.sleep(0.25)

            for w in self._workers:
                w.join(timeout=5)

            if self._stop_event.is_set():
                self._persist()
                return

            with self._lock:
                missing = [c for c in self._chunks if not c["done"]]
            if missing:
                werr = next((w.last_error for w in self._workers if w.last_error), None)
                raise RuntimeError(f"{len(missing)} chunk(s) incomplete" + (f": {werr}" if werr else ""))

            self._finish()

        except Exception as e:
            self.error = str(e)
            self.state = "failed"
            self._persist()

    def _auto_balance(self):
        dur = self.settings["speed_probe_seconds"]
        ua = self.settings["user_agent"]
        hdrs = self.settings.get("custom_headers")
        ct = self.settings["connect_timeout"]
        rt = self.settings["read_timeout"]
        v6_out = [0.0]
        v4_out = [0.0]

        def r6():
            v6_out[0] = speed_probe(self.url, self.ipv6_ip, socket.AF_INET6, ua, hdrs, dur, ct, rt, insecure_ssl=self.settings.get("insecure_ssl", False))

        def r4():
            v4_out[0] = speed_probe(self.url, self.ipv4_ip, socket.AF_INET, ua, hdrs, dur, ct, rt, insecure_ssl=self.settings.get("insecure_ssl", False))

        t6 = threading.Thread(target=r6, daemon=True)
        t4 = threading.Thread(target=r4, daemon=True)
        t6.start(); t4.start()
        t6.join(); t4.join()

        v6, v4 = v6_out[0], v4_out[0]
        total = self.settings["ipv6_workers"] + self.settings["ipv4_workers"]
        if v6 <= 0 and v4 <= 0:
            return
        if v6 <= 0:
            self.settings["ipv6_workers"] = 0
            self.settings["ipv4_workers"] = total
            return
        if v4 <= 0:
            self.settings["ipv6_workers"] = total
            self.settings["ipv4_workers"] = 0
            return
        v6_share = v6 / (v6 + v4)
        v6_n = max(1, round(total * v6_share))
        v4_n = max(1, total - v6_n)
        self.settings["ipv6_workers"] = v6_n
        self.settings["ipv4_workers"] = v4_n

    def _persist(self):
        try:
            state = {
                "url": self.url,
                "final_url": self.final_url,
                "output_path": str(self.output_path),
                "size": self.size,
                "etag": self.etag,
                "last_modified": self.last_modified,
                "chunk_size": self.settings["chunk_size_mb"] * 1024 * 1024,
                "chunks": self._chunks,
                "completed": self.state == "completed",
                "supports_ranges": self.supports_ranges,
                "single_stream": self.single_stream,
            }
            sidecar_save(self.output_path, state)
        except Exception:
            pass

    def _finish(self):
        actual = os.path.getsize(self.output_path)
        if actual != self.size:
            raise RuntimeError(f"size mismatch: expected {self.size}, got {actual}")

        if self.sha256:
            h = hashlib.sha256()
            with open(self.output_path, "rb") as f:
                while True:
                    buf = f.read(1024 * 1024)
                    if not buf:
                        break
                    h.update(buf)
            if h.hexdigest().lower() != self.sha256:
                raise RuntimeError("SHA-256 mismatch")

        self.state = "completed"
        self._persist()
        sidecar_delete(self.output_path)


# ====================================================================
# GUI
# ====================================================================

def fmt_bytes(n):
    if n is None:
        return "?"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def fmt_speed(bps):
    return fmt_bytes(bps) + "/s"


def fmt_eta(sec):
    if sec is None or sec < 0:
        return "--"
    sec = int(sec)
    h, sec = divmod(sec, 3600)
    m, s = divmod(sec, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def suggest_filename(url):
    p = urllib.parse.urlparse(url)
    return os.path.basename(urllib.parse.unquote(p.path)) or "download.bin"


class OptionsDialog(tk.Toplevel):
    def __init__(self, parent, settings, on_save):
        super().__init__(parent)
        self.title("Options")
        self.resizable(False, False)
        self.settings = dict(settings)
        self.on_save = on_save

        nb = ttk.Notebook(self)
        nb.pack(padx=10, pady=10, fill="both", expand=True)

        # Connections
        conn = ttk.Frame(nb, padding=10); nb.add(conn, text="Connections")
        self.v6 = tk.IntVar(value=self.settings["ipv6_workers"])
        self.v4 = tk.IntVar(value=self.settings["ipv4_workers"])
        self.chunk = tk.IntVar(value=self.settings["chunk_size_mb"])
        self.retries = tk.IntVar(value=self.settings["max_retries"])
        self.ct = tk.IntVar(value=self.settings["connect_timeout"])
        self.rt = tk.IntVar(value=self.settings["read_timeout"])
        self.keepalive = tk.BooleanVar(value=self.settings["keepalive_enabled"])
        self.probe = tk.BooleanVar(value=self.settings["speed_probe_enabled"])
        self.insecure_ssl = tk.BooleanVar(value=self.settings.get("insecure_ssl", False))
        self.single_fallback = tk.BooleanVar(value=self.settings.get("allow_single_stream_fallback", True))
        self.max_redirects = tk.IntVar(value=self.settings.get("max_redirects", 6))
        rows = [
            ("IPv6 workers", self.v6, 0, 32),
            ("IPv4 workers", self.v4, 0, 32),
            ("Chunk size (MB)", self.chunk, 1, 256),
            ("Max retries", self.retries, 0, 20),
            ("Connect timeout (s)", self.ct, 1, 120),
            ("Read timeout (s)", self.rt, 1, 300),
        ]
        for i, (lbl, var, lo, hi) in enumerate(rows):
            ttk.Label(conn, text=lbl).grid(row=i, column=0, sticky="w", pady=2)
            ttk.Spinbox(conn, from_=lo, to=hi, textvariable=var, width=8).grid(row=i, column=1, sticky="w")
        ttk.Checkbutton(conn, text="HTTP keep-alive", variable=self.keepalive).grid(
            row=len(rows), column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Checkbutton(conn, text="Auto-balance via speed probe", variable=self.probe).grid(
            row=len(rows)+1, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(conn, text="Ignore SSL errors / self-signed certs", variable=self.insecure_ssl).grid(
            row=len(rows)+2, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(conn, text="Allow single-stream fallback when ranges are unavailable", variable=self.single_fallback).grid(
            row=len(rows)+3, column=0, columnspan=2, sticky="w")
        ttk.Label(conn, text="Max redirects").grid(row=len(rows)+4, column=0, sticky="w", pady=(8, 2))
        ttk.Spinbox(conn, from_=0, to=20, textvariable=self.max_redirects, width=8).grid(row=len(rows)+4, column=1, sticky="w")

        # Stall / Limit
        stall = ttk.Frame(nb, padding=10); nb.add(stall, text="Stall / Limit")
        self.stall_kbps = tk.IntVar(value=self.settings["stall_threshold_kbps"])
        self.stall_win = tk.IntVar(value=self.settings["stall_window_sec"])
        self.limit = tk.IntVar(value=self.settings["speed_limit_kbps"])
        for i, (lbl, var, hi) in enumerate([
            ("Stall threshold (KB/s)", self.stall_kbps, 100000),
            ("Stall window (sec)", self.stall_win, 120),
            ("Speed limit (KB/s, 0=unlimited)", self.limit, 1000000),
        ]):
            ttk.Label(stall, text=lbl).grid(row=i, column=0, sticky="w", pady=2)
            ttk.Spinbox(stall, from_=0, to=hi, textvariable=var, width=10).grid(row=i, column=1, sticky="w")

        # Headers
        hdr = ttk.Frame(nb, padding=10); nb.add(hdr, text="Headers")
        ttk.Label(hdr, text="User-Agent:").grid(row=0, column=0, sticky="w")
        self.ua = tk.StringVar(value=self.settings["user_agent"])
        ttk.Entry(hdr, textvariable=self.ua, width=50).grid(row=0, column=1, sticky="we", pady=2)
        ttk.Label(hdr, text="Custom headers (Key: Value per line):").grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 2))
        self.hdr_box = tk.Text(hdr, height=6, width=50)
        for k, v in (self.settings.get("custom_headers") or {}).items():
            self.hdr_box.insert("end", f"{k}: {v}\n")
        self.hdr_box.grid(row=2, column=0, columnspan=2, sticky="we")

        # General
        gen = ttk.Frame(nb, padding=10); nb.add(gen, text="General")
        ttk.Label(gen, text="Default folder:").grid(row=0, column=0, sticky="w")
        self.out_folder = tk.StringVar(value=self.settings["output_folder"])
        ttk.Entry(gen, textvariable=self.out_folder, width=40).grid(row=0, column=1, sticky="we", pady=2)
        ttk.Button(gen, text="Browse", command=self._browse).grid(row=0, column=2, padx=4)
        ttk.Label(gen, text="Max concurrent downloads:").grid(row=1, column=0, sticky="w")
        self.max_concurrent = tk.IntVar(value=self.settings["max_concurrent_downloads"])
        ttk.Spinbox(gen, from_=1, to=16, textvariable=self.max_concurrent, width=8).grid(row=1, column=1, sticky="w")
        self.shutdown = tk.BooleanVar(value=self.settings["auto_shutdown"])
        ttk.Checkbutton(gen, text="Shutdown PC when all downloads finish", variable=self.shutdown).grid(
            row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))

        btns = ttk.Frame(self); btns.pack(pady=(0, 10))
        ttk.Button(btns, text="Save", command=self._save).pack(side="left", padx=4)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side="left", padx=4)

    def _browse(self):
        d = filedialog.askdirectory(initialdir=self.out_folder.get())
        if d:
            self.out_folder.set(d)

    def _save(self):
        headers = {}
        for line in self.hdr_box.get("1.0", "end").strip().splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip()] = v.strip()
        new = dict(self.settings)
        new.update({
            "ipv6_workers": self.v6.get(),
            "ipv4_workers": self.v4.get(),
            "chunk_size_mb": self.chunk.get(),
            "max_retries": self.retries.get(),
            "connect_timeout": self.ct.get(),
            "read_timeout": self.rt.get(),
            "keepalive_enabled": self.keepalive.get(),
            "speed_probe_enabled": self.probe.get(),
            "insecure_ssl": self.insecure_ssl.get(),
            "allow_single_stream_fallback": self.single_fallback.get(),
            "max_redirects": self.max_redirects.get(),
            "stall_threshold_kbps": self.stall_kbps.get(),
            "stall_window_sec": self.stall_win.get(),
            "speed_limit_kbps": self.limit.get(),
            "user_agent": self.ua.get(),
            "custom_headers": headers,
            "output_folder": self.out_folder.get(),
            "max_concurrent_downloads": self.max_concurrent.get(),
            "auto_shutdown": self.shutdown.get(),
        })
        self.on_save(new)
        self.destroy()


class AddDownloadDialog(tk.Toplevel):
    def __init__(self, parent, settings, on_add):
        super().__init__(parent)
        self.title("Add download")
        self.resizable(False, False)
        self.on_add = on_add

        frm = ttk.Frame(self, padding=10); frm.pack()
        ttk.Label(frm, text="URL:").grid(row=0, column=0, sticky="w")
        self.url = tk.StringVar()
        ttk.Entry(frm, textvariable=self.url, width=60).grid(row=0, column=1, columnspan=2, sticky="we", pady=2)
        self.url.trace_add("write", self._url_changed)

        ttk.Label(frm, text="Filename:").grid(row=1, column=0, sticky="w")
        self.filename = tk.StringVar()
        ttk.Entry(frm, textvariable=self.filename, width=60).grid(row=1, column=1, columnspan=2, sticky="we", pady=2)

        ttk.Label(frm, text="Folder:").grid(row=2, column=0, sticky="w")
        self.folder = tk.StringVar(value=settings["output_folder"])
        ttk.Entry(frm, textvariable=self.folder, width=50).grid(row=2, column=1, sticky="we", pady=2)
        ttk.Button(frm, text="Browse", command=self._browse).grid(row=2, column=2, padx=4)

        ttk.Label(frm, text="SHA-256 (optional):").grid(row=3, column=0, sticky="w")
        self.sha = tk.StringVar()
        ttk.Entry(frm, textvariable=self.sha, width=60).grid(row=3, column=1, columnspan=2, sticky="we", pady=2)

        btns = ttk.Frame(frm); btns.grid(row=4, column=0, columnspan=3, pady=(10, 0))
        ttk.Button(btns, text="Add", command=self._add).pack(side="left", padx=4)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side="left", padx=4)

        try:
            c = parent.clipboard_get()
            if c.startswith(("http://", "https://")):
                self.url.set(c)
        except tk.TclError:
            pass

    def _url_changed(self, *_):
        if not self.filename.get():
            u = self.url.get().strip()
            if u:
                self.filename.set(suggest_filename(u))

    def _browse(self):
        d = filedialog.askdirectory(initialdir=self.folder.get())
        if d:
            self.folder.set(d)

    def _add(self):
        u = self.url.get().strip()
        f = self.filename.get().strip()
        folder = self.folder.get().strip()
        if not u or not f or not folder:
            messagebox.showerror("Missing", "URL, filename and folder are required")
            return
        Path(folder).mkdir(parents=True, exist_ok=True)
        out = os.path.join(folder, f)
        self.on_add(u, out, self.sha.get().strip() or None)
        self.destroy()


class MainWindow:
    def __init__(self, root):
        self.root = root
        root.title("Dual-Stack Download Accelerator")
        root.geometry("950x560")

        self.settings = load_settings()
        self.downloads = []

        bar = ttk.Frame(root, padding=(8, 6)); bar.pack(fill="x")
        ttk.Button(bar, text="+ Add", command=self._add).pack(side="left", padx=2)
        ttk.Button(bar, text="▶ Start", command=self._start).pack(side="left", padx=2)
        ttk.Button(bar, text="⏸ Pause", command=self._pause).pack(side="left", padx=2)
        ttk.Button(bar, text="■ Cancel", command=self._cancel).pack(side="left", padx=2)
        ttk.Button(bar, text="✕ Remove", command=self._remove).pack(side="left", padx=2)
        ttk.Button(bar, text="⚙ Options", command=self._options).pack(side="right", padx=2)

        cols = ("file", "size", "progress", "speed", "eta", "state")
        self.tree = ttk.Treeview(root, columns=cols, show="headings", height=10)
        widths = {"file": 300, "size": 90, "progress": 80, "speed": 110, "eta": 80, "state": 100}
        for c in cols:
            self.tree.heading(c, text=c.title())
            self.tree.column(c, width=widths[c], anchor="w")
        self.tree.pack(fill="x", padx=8, pady=4)
        self.tree.bind("<<TreeviewSelect>>", lambda e: self._refresh_detail())

        df = ttk.LabelFrame(root, text="Detail", padding=8)
        df.pack(fill="both", expand=True, padx=8, pady=(4, 8))
        self.detail = tk.Text(df, height=12, state="disabled", font=("Consolas", 9))
        self.detail.pack(fill="both", expand=True)

        self.root.after(300, self._tick)

    def _add(self):
        AddDownloadDialog(self.root, self.settings, self._on_add)

    def _on_add(self, url, out, sha):
        d = Download(url, out, self.settings, sha256=sha)
        self.downloads.append(d)
        self.tree.insert("", "end", iid=str(id(d)),
                         values=(os.path.basename(out), "?", "0%", "", "", "pending"))
        self._maybe_start_more()

    def _selected(self):
        sel = self.tree.selection()
        out = []
        for iid in sel:
            for d in self.downloads:
                if str(id(d)) == iid:
                    out.append(d)
        return out

    def _start(self):
        for d in self._selected():
            if d.state == "paused":
                d.resume()
            elif d.state in ("pending", "failed", "cancelled"):
                d.start()

    def _pause(self):
        for d in self._selected():
            d.pause()

    def _cancel(self):
        for d in self._selected():
            d.cancel()

    def _remove(self):
        for d in self._selected():
            d.cancel()
            try:
                self.tree.delete(str(id(d)))
            except Exception:
                pass
            if d in self.downloads:
                self.downloads.remove(d)

    def _options(self):
        OptionsDialog(self.root, self.settings, self._save_settings)

    def _save_settings(self, new):
        self.settings = new
        save_settings(new)

    def _maybe_start_more(self):
        cap = self.settings["max_concurrent_downloads"]
        active = sum(1 for d in self.downloads if d.state in ("probing", "running"))
        for d in self.downloads:
            if active >= cap:
                break
            if d.state == "pending":
                d.start()
                active += 1

    def _tick(self):
        for d in self.downloads:
            try:
                self.tree.item(str(id(d)), values=(
                    os.path.basename(d.output_path),
                    fmt_bytes(d.size) if d.size else "?",
                    f"{d.progress_pct:.1f}%",
                    fmt_speed(d.overall_speed) if d.state == "running" else "",
                    fmt_eta(d.eta_seconds) if d.state == "running" else "",
                    d.state,
                ))
            except Exception:
                pass
        self._refresh_detail()
        self._maybe_start_more()
        self.root.after(400, self._tick)

    def _refresh_detail(self):
        sel = self._selected()
        self.detail.config(state="normal")
        self.detail.delete("1.0", "end")
        if sel:
            d = sel[0]
            lines = [
                f"URL:        {d.url}",
                f"File:       {d.output_path}",
                f"Size:       {fmt_bytes(d.size)}  ({d.size} bytes)",
                f"ETag:       {d.etag or '-'}",
                f"IPv4:       {d.ipv4_ip or '-'}",
                f"IPv6:       {d.ipv6_ip or '-'}",
                f"Dual-stack: {'yes' if d.dual_stack else 'no'}",
                f"Mode:       {'ranged multi-connection' if d.supports_ranges else 'single-stream fallback'}",
                f"SSL verify: {'strict' if not d.settings.get("insecure_ssl", False) else 'disabled'}",
                f"State:      {d.state}" + (f"    error: {d.error}" if d.error else ""),
                f"Speed:      v6 {fmt_speed(d.speed_v6)}    v4 {fmt_speed(d.speed_v4)}    total {fmt_speed(d.overall_speed)}",
                "",
                "Workers:",
            ]
            for w in d.worker_stats():
                err = f"  err: {w['error']}" if w["error"] else ""
                ck = f"chunk {w['active_chunk']}" if w["active_chunk"] is not None else ""
                lines.append(
                    f"  {w['name']:8} {w['state']:8} {fmt_speed(w['speed']):>14}  "
                    f"{fmt_bytes(w['bytes']):>10}  [{w['ip']}]  {ck}{err}"
                )
            self.detail.insert("1.0", "\n".join(lines))
        self.detail.config(state="disabled")


def main():
    root = tk.Tk()
    MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
