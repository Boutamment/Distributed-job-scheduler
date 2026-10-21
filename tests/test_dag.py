import pytest

from app.dag import DagError, validate


def t(key, *deps):
    return {"key": key, "depends_on": list(deps)}


def test_valid_order_respects_dependencies():
    order = validate([t("load", "clean", "validate"), t("clean", "extract"),
                      t("validate", "extract"), t("extract")])
    assert order.index("extract") < order.index("clean") < order.index("load")
    assert order.index("validate") < order.index("load")


def test_independent_tasks_ok():
    assert sorted(validate([t("a"), t("b")])) == ["a", "b"]


def test_cycle_rejected():
    with pytest.raises(DagError, match="cycle"):
        validate([t("a", "c"), t("b", "a"), t("c", "b")])


def test_self_dependency_rejected():
    with pytest.raises(DagError, match="itself"):
        validate([t("a", "a")])


def test_unknown_dependency_rejected():
    with pytest.raises(DagError, match="unknown"):
        validate([t("a", "ghost")])


def test_duplicate_keys_rejected():
    with pytest.raises(DagError, match="duplicate"):
        validate([t("a"), t("a")])
