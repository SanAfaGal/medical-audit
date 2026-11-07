"""Guards against server-side request forgery (SSRF) for user-configurable portal URLs.

The SIHOS portal URL is entered by the user and then opened by a server-side browser. Without
checks it could be pointed at the host itself or at cloud metadata endpoints. Private (RFC 1918)
addresses stay allowed on purpose: hospital SIHOS servers normally live on the LAN.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from urllib.parse import urlsplit

_BLOCKED_NAMES = frozenset({"localhost", "metadata.google.internal", "metadata", "ip6-localhost"})


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved


# Browsers (and inet_aton) also read legacy IPv4 spellings such as "2130706433", "0x7f.1" or "127.1".
_LEGACY_IP_RE = re.compile(r"[0-9a-fA-FxX.]+")


def _parse_ip(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """Parse an IP literal the way a browser would, including legacy IPv4 notations."""
    host = host.strip("[]")
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        pass
    if _LEGACY_IP_RE.fullmatch(host):
        try:
            return ipaddress.IPv4Address(socket.inet_aton(host))
        except OSError:
            return None
    return None


def _blocked_host(host: str) -> bool:
    host = host.strip("[]").lower().rstrip(".")
    if host in _BLOCKED_NAMES or host.endswith(".localhost"):
        return True
    ip = _parse_ip(host)
    return ip is not None and _is_blocked_ip(ip)  # None: a hostname, not an IP literal


def is_blocked_url(url: str) -> bool:
    """True if an http(s) URL targets a blocked host (checked without DNS). Other schemes are ignored."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return False
    return _blocked_host(parts.hostname)


async def is_blocked_request(url: str, cache: dict[str, bool]) -> bool:
    """Like :func:`is_blocked_url`, but also resolves hostnames (cached per host in ``cache``).

    Used by the browser request guard so a redirect to a name that points at loopback/metadata is
    stopped too. Resolution here and the browser's own lookup are separate, so a hostile DNS server
    that changes its answer between them (DNS rebinding) is not fully covered.
    """
    parts = urlsplit(url)
    host = parts.hostname
    if parts.scheme not in ("http", "https") or not host:
        return False
    if _blocked_host(host):
        return True
    if _parse_ip(host) is not None:
        return False  # public IP literal, already checked above
    if host in cache:
        return cache[host]
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None)
        blocked = any(_is_blocked_ip(ipaddress.ip_address(i[4][0].split("%")[0])) for i in infos)
    except (OSError, ValueError):
        blocked = False  # unresolvable: the browser will fail on its own
    cache[host] = blocked
    return blocked


def validate_portal_url(url: str, *, resolve: bool = False) -> str:
    """Return ``url`` if it is a safe http(s) portal address, otherwise raise ``ValueError``.

    With ``resolve=True`` the hostname is also resolved and every returned address is checked, which
    catches names that point at loopback/link-local addresses. Call that just before connecting.
    """
    parts = urlsplit(url.strip())
    if parts.scheme not in ("http", "https"):
        raise ValueError("La URL debe empezar por http:// o https://")
    host = parts.hostname
    if not host:
        raise ValueError("La URL no tiene un host válido")
    if parts.username or parts.password:
        raise ValueError("La URL no debe incluir usuario ni contraseña")
    if _blocked_host(host):
        raise ValueError("La URL apunta a una dirección no permitida (loopback, enlace local o metadatos)")
    if resolve:
        try:
            infos = socket.getaddrinfo(host, parts.port or (443 if parts.scheme == "https" else 80))
        except socket.gaierror as exc:
            raise ValueError(f"No se pudo resolver el host '{host}'") from exc
        for info in infos:
            if _is_blocked_ip(ipaddress.ip_address(info[4][0])):
                raise ValueError(f"El host '{host}' resuelve a una dirección no permitida")
    return url
