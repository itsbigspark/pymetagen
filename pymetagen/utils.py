from __future__ import annotations

from typing import Any


class EnumListMixin:
    @classmethod
    def list(cls) -> list[str]:
        return list(map(lambda c: c.value, cls))


def selectively_update_dict(d: dict[str, Any], new_d: dict[str, Any]) -> None:
    """
    Selectively update dictionary d with any values that are in new_d,
    but being careful only to update keys in dictionaries that are present
    in new_d.

    Args:
        d: dictionary with string keys
        new_d: dictionary with string keys
    """
    for k, v in new_d.items():
        if isinstance(v, dict) and k in d:
            if isinstance(d[k], dict):
                selectively_update_dict(d[k], v)
            else:
                d[k] = v
        else:
            d[k] = v
