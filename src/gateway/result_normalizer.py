"""Normalize raw worker result payloads into client-friendly messages."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)

ResultMessage = Dict[str, Any]


def _normalize_quantity_profit_bundle(bundle: Dict[str, Any]) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    quantity_rows = bundle.get("quantity")
    if isinstance(quantity_rows, list):
        normalized.append(
            {
                "type": "TOP_ITEMS_BY_QUANTITY",
                "results": quantity_rows,
            }
        )

    profit_rows = bundle.get("profit")
    if isinstance(profit_rows, list):
        normalized.append(
            {
                "type": "TOP_ITEMS_BY_PROFIT",
                "results": profit_rows,
            }
        )

    return normalized


def _normalize_data_list(rows: List[Any]) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    if not rows:
        return normalized

    first_entry = rows[0]
    if isinstance(first_entry, dict) and {"quantity", "profit"}.issubset(first_entry.keys()):
        # Replica bundle containing quantity/profit lists.
        return _normalize_quantity_profit_bundle(first_entry)

    if not all(isinstance(row, dict) for row in rows):
        return normalized

    sample_keys = {key for key in rows[0] if isinstance(key, str)}

    if "transaction_id" in sample_keys or "final_amount" in sample_keys:
        normalized.append(
            {
                "type": "AMOUNT_FILTER_TRANSACTIONS",
                "results": rows,
            }
        )
        return normalized

    if {"year_month_created_at", "sellings_qty"}.issubset(sample_keys):
        normalized.append(
            {
                "type": "TOP_ITEMS_BY_QUANTITY",
                "results": rows,
            }
        )
        return normalized

    if {"year_month_created_at", "profit_sum"}.issubset(sample_keys):
        normalized.append(
            {
                "type": "TOP_ITEMS_BY_PROFIT",
                "results": rows,
            }
        )
        return normalized

    if "tpv" in sample_keys or {"year", "semester", "store_name"}.intersection(sample_keys):
        normalized.append(
            {
                "type": "TPV_SUMMARY",
                "results": rows,
            }
        )
        return normalized

    if {"user_id", "purchases_qty"}.issubset(sample_keys) or "birthdate" in sample_keys:
        normalized.append(
            {
                "type": "TOP_CLIENTS_BIRTHDAYS",
                "results": rows,
            }
        )
        return normalized

    return normalized


def _normalize_data_dict(data: Dict[str, Any]) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    if {"quantity", "profit"}.issubset(data.keys()):
        return _normalize_quantity_profit_bundle(data)

    results_section = data.get("results")
    nested_type = str(data.get("type") or "").upper()

    if isinstance(results_section, list):
        if nested_type:
            normalized.append(
                {
                    "type": nested_type,
                    "results": results_section,
                }
            )
            return normalized

        normalized.extend(_normalize_data_list(results_section))
        return normalized

    return normalized


def _extract_candidate_lists(flattened: Dict[str, Any]) -> Iterable[List[Any]]:
    for value in flattened.values():
        if isinstance(value, list):
            yield value


def normalize_queue_message(message: Dict[str, Any]) -> List[ResultMessage]:
    """Normalize a raw queue message into one or more client payloads."""

    message_type = str(message.get("type") or "").upper()
    if message_type == "EOF":
        return []

    normalized: List[ResultMessage] = []

    data_section = message.get("data")
    if isinstance(data_section, dict):
        normalized.extend(_normalize_data_dict(data_section))
    elif isinstance(data_section, list):
        normalized.extend(_normalize_data_list(data_section))

    if normalized:
        return normalized

    if "results" in message and isinstance(message["results"], list):
        rows = message["results"]
        entry_type = message_type or ""
        if not entry_type:
            extra = _normalize_data_list(rows)
            if extra:
                return extra
        normalized.append(
            {
                "type": entry_type or "UNKNOWN",
                "results": rows,
            }
        )
        return normalized

    flattened = {k: v for k, v in message.items() if k not in {"client_id", "type", "data"}}
    if isinstance(flattened, dict) and flattened:
        normalized.extend(_normalize_data_dict(flattened))
        if normalized:
            return normalized
        for candidate in _extract_candidate_lists(flattened):
            extra = _normalize_data_list(candidate)
            if extra:
                normalized.extend(extra)
        if normalized:
            return normalized

    if message_type:
        logger.debug("No specific normalization matched; using fallback for type %s", message_type)
        normalized.append(
            {
                "type": message_type,
                "results": [],
            }
        )

    return normalized
