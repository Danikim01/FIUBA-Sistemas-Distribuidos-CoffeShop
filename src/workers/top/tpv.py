#!/usr/bin/env python3

"""TPV worker that aggregates semester totals per store."""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict
from message_utils import ClientId
from worker_utils import extract_year_half, run_main, safe_float_conversion, safe_int_conversion
from workers.top.top_worker import TopWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

YearHalf = str
StoreId = int

class TPVWorker(TopWorker):
    """Computes Total Payment Value per semester for each store."""

    def __init__(self) -> None:
        super().__init__()
        self._tpv_totals: DefaultDict[
            ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        logger.info("TPVWorker initialized")


    def _accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        year_half: YearHalf | None = extract_year_half(payload.get('created_at'))
        if not year_half:
            return

        try:
            store_id: StoreId = safe_int_conversion(payload.get('store_id'), minimum=0)
        except Exception:  # noqa: BLE001
            logger.debug("Transaction without valid store_id: %s", payload)
            return

        amount: float = safe_float_conversion(payload.get('final_amount'), 0.0)

        bucket = self._tpv_totals[client_id][year_half]
        bucket[store_id] += amount

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        totals = self._tpv_totals.get(client_id, {})
        results: list[Dict[str, Any]] = []

        for year_half, stores in totals.items():
            for store_id, tpv_value in stores.items():
                results.append(
                    {
                        'year_half_created_at': year_half,
                        'store_id': store_id,
                        'tpv': tpv_value,
                    }
                )

        return results

 

if __name__ == '__main__':
    run_main(TPVWorker)
