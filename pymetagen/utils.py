from __future__ import annotations

import datetime
import sys
from typing import Any


def nvl(v, default):
    return default if v is None else v


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


def flatten(
    o: Any, ref: Any, key: str | None = None, exclude_none: bool = False
):
    if isinstance(o, dict):
        return {
            k: flatten(v, ref[k], key=k)
            for k, v in o.items()
            if k in ref and (v is not None or not exclude_none)
        }
    elif isinstance(o, list | tuple):
        return [
            flatten(v, w, key=str(i)) for i, (v, w) in enumerate(zip(o, ref))
        ]
    elif o is None or type(o) in (
        bool,
        str,
        int,
        float,
        datetime.date,
        datetime.time,
    ):
        return o
    else:
        if key is None:
            print(
                f"Cannot flatten object type {type(o)}:\n{str(o)}\nSkipping!!",
                file=sys.stderr,
            )
        else:
            print(
                f"Cannot flatten object type {type(o)} for"
                f" {key}\n\nSkipping!!",
                file=sys.stderr,
            )
