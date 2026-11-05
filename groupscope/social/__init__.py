"""Phase 2：人物与关系。

* ``extract`` — 从消息中抽取确定性互动（@ / 回复 / 引用）
* ``snapshots`` — 按时间窗口聚合关系快照
* ``profiles`` — 成员档案、关系矩阵、人工身份合并

注意：本包 ``__init__`` 保持轻量，不在此处导入 ``storage``，
避免与 ``storage.repo`` 形成循环导入。
"""

__all__ = [
    "extract_interactions",
    "member_profile",
    "member_search",
    "merge_members",
    "relation_matrix",
    "compute_snapshots",
    "snapshot_windows",
]


def __getattr__(name: str):
    if name in ("extract_interactions",):
        from .extract import extract_interactions

        return extract_interactions
    if name in ("member_profile", "member_search", "merge_members", "relation_matrix"):
        from . import profiles

        return getattr(profiles, name)
    if name in ("compute_snapshots", "snapshot_windows"):
        from . import snapshots

        return getattr(snapshots, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
