import logging
from urllib.parse import quote

import goldlapel
from django.db.backends.postgresql.base import DatabaseWrapper as PgDatabaseWrapper

logger = logging.getLogger("goldlapel.django")


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

    return f"postgresql://{userinfo}{host}:{port}/{name}"


class DatabaseWrapper(PgDatabaseWrapper):
    def get_connection_params(self):
        params = super().get_connection_params()

        gl_opts = params.pop("goldlapel", {})
        # Django OPTIONS dict uses the canonical snake_case surface —
        # `proxy_port`, `dashboard_port`, `log_level`, `mode`, etc. —
        # matching `goldlapel.start(**opts)`. Without a configured
        # proxy_port the core picks one, so several DATABASES each get
        # their own proxy + dashboard pair.
        start_kwargs = {"client": "django"}
        for key in (
            "proxy_port", "dashboard_port", "log_level", "mode",
            "license", "config_file", "config", "extra_args", "silent",
        ):
            if key in gl_opts:
                start_kwargs[key] = gl_opts[key]

        upstream = _build_upstream_url(self.settings_dict)

        try:
            gl = goldlapel.start(upstream, **start_kwargs)
            params["host"] = "127.0.0.1"
            params["port"] = gl.proxy_port
        except Exception as exc:
            logger.warning(
                "Gold Lapel proxy failed to start, falling back to direct connection: %s",
                exc,
            )

        return params
