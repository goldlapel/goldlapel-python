import logging
from urllib.parse import quote

import goldlapel
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.postgresql.base import DatabaseWrapper as PgDatabaseWrapper
from goldlapel.proxy import _UPSTREAM_ONLY_PARAMS, _client_tls, _unknown_options_message

logger = logging.getLogger("goldlapel.django")

# OPTIONS["goldlapel"] takes the keyword options of `goldlapel.start(**opts)`
# under the same snake_case names.
_START_OPTIONS = (
    "proxy_port", "dashboard_port", "log_level", "mode", "license",
    "api_key", "client", "config_file", "config", "extra_args",
    "silent", "mesh", "mesh_tag", "disable_proxy_cache",
    "disable_sqloptimize", "disable_auto_indexes",
)


def _build_upstream_url(settings):
    host = settings.get("HOST") or "localhost"
    port = str(settings.get("PORT") or 5432)

    if host.startswith("/"):
        raise ValueError(
            f"Gold Lapel cannot proxy Unix socket connections (HOST={host!r}). "
            "Use a TCP host instead."
        )

    user = settings.get("USER")
    password = settings.get("PASSWORD")

    if user:
        userinfo = quote(user, safe="")
        if password:
            userinfo += ":" + quote(password, safe="")
        userinfo += "@"
    else:
        userinfo = ""

    name = quote(settings.get("NAME") or "", safe="")

    # TLS/GSS settings in OPTIONS (sslmode, sslrootcert, ...) are for the
    # database: the proxy takes them on its upstream URL.
    tls = [
        f"{key}={quote(str(value), safe='')}"
        for key, value in (settings.get("OPTIONS") or {}).items()
        if key.lower() in _UPSTREAM_ONLY_PARAMS and value is not None
    ]
    query = "?" + "&".join(tls) if tls else ""

    return f"postgresql://{userinfo}{host}:{port}/{name}{query}"


class DatabaseWrapper(PgDatabaseWrapper):
    def get_connection_params(self):
        params = super().get_connection_params()

        # A configured-but-empty `"goldlapel": None` means no options.
        gl_opts = params.pop("goldlapel", None) or {}
        unknown = set(gl_opts) - set(_START_OPTIONS)
        if unknown:
            raise ImproperlyConfigured(
                _unknown_options_message(unknown) + ' in OPTIONS["goldlapel"]'
            )
        # Without a configured proxy_port the core picks one, so several
        # DATABASES each get their own proxy + dashboard pair.
        start_kwargs = {"client": "django", **gl_opts}

        upstream = _build_upstream_url(self.settings_dict)

        try:
            gl = goldlapel.start(upstream, **start_kwargs)
            params["host"] = "127.0.0.1"
            params["port"] = gl.proxy_port
            # The proxy declines client TLS unless it serves it; the TLS
            # settings went upstream with the URL above.
            if not _client_tls(gl_opts.get("config"), gl_opts.get("extra_args")):
                for key in list(params):
                    if key.lower() in _UPSTREAM_ONLY_PARAMS:
                        del params[key]
        except Exception as exc:
            logger.warning(
                "Gold Lapel proxy failed to start, falling back to direct connection: %s",
                exc,
            )

        return params
