"""Phase 5：WebUI（AstrBot 插件视图 + 扩展 API）。

机制（源码核对）：
* 视图页面：插件根目录 ``views/<name>/index.html`` 被 Dashboard 自动发现，
  通过 ``/api/v1/plugins/<插件名>/views/<name>`` 访问，鉴权复用 Dashboard
  （JWT / plugin-scope API Key），**不自建账号体系**。
* 扩展 API：``context.register_web_api(route, handler, methods, desc)``，
  经 ``/api/v1/plugins/extensions/<route>`` 暴露，入口已挂
  ``require_plugin_scope`` 鉴权；handler 通过 ``astrbot.api.web.request``
  读取 query/body，返回 ``json_response``。
"""

__all__ = ["register_web_api"]
