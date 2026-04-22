from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


_OUTLIER_HANDLING_VALUES = frozenset({"none", "std", "iqr"})


def _parse_outlier_handling(value: Any, *, context: str) -> str:
    if value is None:
        return "std"
    if not isinstance(value, str):
        raise ValueError(f"{context}: expected outlier_handling to be a str, got {type(value).__name__}")
    v = value.strip().lower()
    if v not in _OUTLIER_HANDLING_VALUES:
        raise ValueError(
            f"{context}: outlier_handling must be one of {sorted(_OUTLIER_HANDLING_VALUES)}, got {value!r}"
        )
    return v


@dataclass(frozen=True)
class RegressionConfig:
    gba_included: bool
    log_transform: bool
    pd_only: bool
    outlier_handling: str


def _parse_bool(value: Any, *, field_name: str, context: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{context}: expected {field_name} to be a bool, got {type(value).__name__}")


def _parse_config(obj: Any, *, context: str) -> RegressionConfig:
    if obj is None:
        raise ValueError(f"{context}: expected a mapping for regression settings, got null")
    if not isinstance(obj, dict):
        raise ValueError(f"{context}: expected a mapping, got {type(obj).__name__}")

    allowed = {"gba_included", "log_transform", "pd_only", "outlier_handling"}
    extra = sorted([k for k in obj.keys() if k not in allowed])
    if extra:
        raise ValueError(f"{context}: unknown keys {extra}; allowed keys are {sorted(allowed)}")

    gba_included = _parse_bool(obj.get("gba_included", False), field_name="gba_included", context=context)
    log_transform = _parse_bool(obj.get("log_transform", False), field_name="log_transform", context=context)
    pd_only = _parse_bool(obj.get("pd_only", True), field_name="pd_only", context=context)
    outlier_handling = _parse_outlier_handling(obj.get("outlier_handling"), context=context)
    return RegressionConfig(
        gba_included=gba_included,
        log_transform=log_transform,
        pd_only=pd_only,
        outlier_handling=outlier_handling,
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config YAML not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError(f"Top-level YAML must be a mapping, got {type(data).__name__}")
    return data


def _coerce_projectids_map(obj: Any) -> dict[str, dict[str, Any]]:
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise ValueError(f"projectids must be a mapping, got {type(obj).__name__}")
    out: dict[str, dict[str, Any]] = {}
    for k, v in obj.items():
        pid = str(k)
        if v is None:
            out[pid] = {}
            continue
        if not isinstance(v, dict):
            raise ValueError(f"projectids[{k!r}] must be a mapping, got {type(v).__name__}")
        out[pid] = v
    return out


def _coerce_testnames_map(obj: Any) -> dict[str, dict[str, Any]]:
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise ValueError(f"testnames must be a mapping, got {type(obj).__name__}")
    out: dict[str, dict[str, Any]] = {}
    for k, v in obj.items():
        name = str(k)
        if v is None:
            out[name] = {}
            continue
        if not isinstance(v, dict):
            raise ValueError(f"testnames[{name!r}] must be a mapping, got {type(v).__name__}")
        out[name] = v
    return out


def load_regression_configs(path: Path) -> tuple[RegressionConfig, dict[str, RegressionConfig], dict[str, RegressionConfig]]:
    data = _load_yaml(path)

    allowed_top = {"global", "projectids", "testnames"}
    extra_top = sorted([k for k in data.keys() if k not in allowed_top])
    if extra_top:
        raise ValueError(f"Unknown top-level keys in YAML: {extra_top}. Allowed: {sorted(allowed_top)}")

    global_cfg = _parse_config(data.get("global", {}), context="global")
    projectids_raw = _coerce_projectids_map(data.get("projectids"))
    testnames_raw = _coerce_testnames_map(data.get("testnames"))

    project_cfgs: dict[str, RegressionConfig] = {}
    for pid, cfg_obj in projectids_raw.items():
        merged = {
            "gba_included": global_cfg.gba_included,
            "log_transform": global_cfg.log_transform,
            "pd_only": global_cfg.pd_only,
            "outlier_handling": global_cfg.outlier_handling,
        }
        merged.update(cfg_obj)
        project_cfgs[pid] = _parse_config(merged, context=f"projectids[{pid}]")

    test_cfgs: dict[str, RegressionConfig] = {}
    for testname, cfg_obj in testnames_raw.items():
        merged = {
            "gba_included": global_cfg.gba_included,
            "log_transform": global_cfg.log_transform,
            "pd_only": global_cfg.pd_only,
            "outlier_handling": global_cfg.outlier_handling,
        }
        merged.update(cfg_obj)
        test_cfgs[testname] = _parse_config(merged, context=f"testnames[{testname!r}]")

    return global_cfg, project_cfgs, test_cfgs


def effective_config(
    *,
    project_id: str | None,
    testname: str,
    global_cfg: RegressionConfig,
    project_cfgs: dict[str, RegressionConfig],
    test_cfgs: dict[str, RegressionConfig],
) -> RegressionConfig:
    if testname in test_cfgs:
        return test_cfgs[testname]
    if project_id is not None and project_id in project_cfgs:
        return project_cfgs[project_id]
    return global_cfg
