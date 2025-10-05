#!/usr/bin/env python3

"""Aggregator that enriches top clients with birthdays and re-ranks globally."""

import logging
from typing import Any, Dict
from message_utils import ClientId
from worker_utils import run_main
from workers.aggregator.extra_source.stores import StoresExtraSource
from workers.top.top_worker import TopWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopClientsBirthdaysAggregator(TopWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()
        self.stores_source = StoresExtraSource(self.middleware_config)
        self.stores_source.start_consuming()
        self.birthdays_source = StoresExtraSource(self.middleware_config)
        self.birthdays_source.start_consuming()
        self.recieved_payloads: Dict[ClientId, list[dict[str, Any]]] = {}

    def reset_state(self, client_id: ClientId) -> None:
        self.recieved_payloads[client_id] = []
    
    def accumulate_transaction(self, client_id: str, payload: dict[str, Any]) -> None:
        self.recieved_payloads.setdefault(client_id, []).append(payload)

    def create_payload(self, client_id: ClientId) -> list[Dict[str, Any]]:
        client_payloads = self.recieved_payloads.pop(client_id, [])
        aggregated: Dict[int, Dict[str, Any]] = {}

        for payload in client_payloads:
            store_id = str(payload.get("store_id", ""))
            user_id = int(payload.get("user_id", 0))
            purchase_qty = int(payload.get("purchase_qty", 0))

            if store_id and user_id > 0:
                if user_id not in aggregated:
                    aggregated[user_id] = {
                        "user_id": user_id,
                        "total_purchase_qty": 0,
                        "store_id": store_id,
                    }
                aggregated[user_id]["total_purchase_qty"] += purchase_qty

        # Enrich with birthdays and store names
        results: list[Dict[str, Any]] = []
        for user_data in aggregated.values():
            user_id = user_data["user_id"]
            store_id = user_data["store_id"]

            birthday = self.birthdays_source.get_item_when_done(client_id, str(user_id))
            store_name = self.stores_source.get_item_when_done(client_id, store_id)

            result_entry = {
                "user_id": user_id,
                "total_purchase_qty": user_data["total_purchase_qty"],
                "store_id": store_id,
                "store_name": store_name,
                "birthday": birthday,
            }
            results.append(result_entry)

        # Sort by total_purchase_qty desc, then by user_id asc
        results.sort(key=lambda x: (-x["total_purchase_qty"], x["user_id"]))

        return results


    def cleanup(self):
        super().cleanup()
        try:
            self.stores_source.close()
        except Exception:  # noqa: BLE001
            logger.warning("Failed to close extra input queue", exc_info=True)


if __name__ == '__main__':
    run_main(TopClientsBirthdaysAggregator)
