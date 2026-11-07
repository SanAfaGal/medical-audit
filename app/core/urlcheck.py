"""Guards against server-side request forgery (SSRF) for user-configurable portal URLs.

The SIHOS portal URL is entered by the user and then opened by a server-side browser. Without
checks it could be pointed at the host itself or at cloud metadata endpoints. Private (RFC 1918)
addresses stay allowed on purpose: hospital SIHOS servers normally live on the LAN.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit

_BLOCKED_NAMES = frozenset({"localhost", "metadata.google.internal", "metadata", "ip6-localhost"})


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_loopback or ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved


def _blocked_host(host: str) -> bool:
    host = host.strip("[]").lower().rstrip(".")
    if host in _BLOCKED_NAMES or host.endswith(".localhost"):
        return True
    try:
        return _is_blocked_ip(ipaddress.ip_address(host))
    except ValueError:
        return False  # a hostname, not an IP literal


def is_blocked_url(url: str) -> bool:
    """True if an http(s) URL targets a blocked host (checked without DNS). Other schemes are ignored."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return False
    return _blocked_host(parts.hostname)


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
