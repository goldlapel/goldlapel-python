import logging
import os
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
    _gl_port = goldlapel.DEFAULT_PORT
    _gl_active = False

    def get_connection_params(self):
        params = super().get_connection_params()

        gl_opts = params.pop("goldlapel", {})
        self._gl_port = gl_opts.get("port", goldlapel.DEFAULT_PORT)
        gl_config = gl_opts.get("config")
        gl_extra_args = gl_opts.get("extra_args")

        upstream = _build_upstream_url(self.settings_dict)
        os.environ["GOLDLAPEL_CLIENT"] = "django"

        try:
            goldlapel.start(upstream, config=gl_config, port=self._gl_port, extra_args=gl_extra_args)
            self._gl_active = True
            params["host"] = "127.0.0.1"
            params["port"] = self._gl_port
        except Exception as exc:
            logger.warning(
                "Gold Lapel proxy failed to start, falling back to direct connection: %s",
                exc,
            )
            self._gl_active = False

        return params

    def get_new_connection(self, conn_params):
        conn = super().get_new_connection(conn_params)
        if not self._gl_active:
            return conn
        gl_opts = self.settings_dict.get("OPTIONS", {}).get("goldlapel", {})
        inv_port = gl_opts.get("invalidation_port", self._gl_port + 2)
        return goldlapel.wrap(conn, invalidation_port=inv_port)
