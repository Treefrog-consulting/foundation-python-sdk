"""Builder for creating a loss group and its loss sets, used by
Program.create_loss_group(). Orchestrates the full upload flow: create
group, parallel per-set upload to S3, poll for terminal status, then
attach the group to the program."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Literal, Mapping, Optional, Union

import requests

from ..csv_builder import build_csv
from ..exceptions import LossGroupAttachError, LossSetValidationError
from .loss_set_result import LossGroupUploadResult, LossSetResult

if TYPE_CHECKING:
    from .foundation_client import FoundationClient


LossTypeStr = Literal["yelt", "elt"]
_YELT_LOSS_TYPE_ID = 2
_ELT_LOSS_TYPE_ID = 4


# --- Upload-status constants mirroring Foundation.Loss.Data.Enums.UploadStatusEnum ---
_STATUS_SUBMIT = 1
_STATUS_READY = 3
_STATUS_ERROR = 4


@dataclass
class _QueuedLossSet:
    name: str
    loss_type: LossTypeStr
    csv_bytes: bytes
    currency_code: str
    event_set_id: int
    perspective_id: int
    variant_id: int
    vendor_id: int
    sim_years: int
    note: Optional[str] = None
    nmp_flag: bool = False
    nmp_model_id: Optional[int] = None
    event_split_view_id: Optional[int] = None


class LossGroup:
    def __init__(self, client: "FoundationClient", program_id: int, name: str):
        if not name or not name.strip():
            raise LossSetValidationError("name is required")
        self._client = client
        self._program_id = program_id
        self._name = name
        self._queued: list[_QueuedLossSet] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def program_id(self) -> int:
        return self._program_id

    def add_yelt(
        self,
        *,
        name: str,
        data: Any,
        currency_code: str,
        event_set_id: Union[int, str],
        perspective_id: Union[int, str],
        variant_id: Union[int, str],
        vendor_id: Union[int, str],
        sim_years: int = 0,
        note: Optional[str] = None,
        nmp_flag: bool = False,
        nmp_model_id: Optional[int] = None,
        event_split_view_id: Optional[int] = None,
        column_map: Optional[Mapping[str, str]] = None,
    ) -> "LossGroup":
        self._add(
            "yelt",
            name=name,
            data=data,
            currency_code=currency_code,
            event_set_id=event_set_id,
            perspective_id=perspective_id,
            variant_id=variant_id,
            vendor_id=vendor_id,
            sim_years=sim_years,
            note=note,
            nmp_flag=nmp_flag,
            nmp_model_id=nmp_model_id,
            event_split_view_id=event_split_view_id,
            column_map=column_map,
        )
        return self

    def add_elt(
        self,
        *,
        name: str,
        data: Any,
        currency_code: str,
        event_set_id: Union[int, str],
        perspective_id: Union[int, str],
        variant_id: Union[int, str],
        vendor_id: Union[int, str],
        sim_years: int = 0,
        note: Optional[str] = None,
        nmp_flag: bool = False,
        nmp_model_id: Optional[int] = None,
        event_split_view_id: Optional[int] = None,
        column_map: Optional[Mapping[str, str]] = None,
    ) -> "LossGroup":
        self._add(
            "elt",
            name=name,
            data=data,
            currency_code=currency_code,
            event_set_id=event_set_id,
            perspective_id=perspective_id,
            variant_id=variant_id,
            vendor_id=vendor_id,
            sim_years=sim_years,
            note=note,
            nmp_flag=nmp_flag,
            nmp_model_id=nmp_model_id,
            event_split_view_id=event_split_view_id,
            column_map=column_map,
        )
        return self

    def upload(
        self,
        poll_interval: int = 10,
        timeout: int = 600,
        max_workers: int = 8,
    ) -> "LossGroupUploadResult":
        if not self._queued:
            raise LossSetValidationError("no loss sets queued; call add_yelt/add_elt first")

        session = self._client._session
        base = self._client.base_url
        port = self._client.PORTS["loss"]
        deal_port = self._client.PORTS["deal"]

        # 1) Create LossSetGroup.
        group_resp = session.post(
            f"{base}:{port}/api/LossSetGroups",
            json={"name": self._name},
        )
        group_resp.raise_for_status()
        loss_set_group_id = int(group_resp.json()["id"])

        # 2) Parallel upload + poll per set.
        workers = max(1, min(len(self._queued), max_workers))
        results: list[LossSetResult] = [None] * len(self._queued)  # type: ignore[list-item]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    self._upload_one,
                    queued=q,
                    loss_set_group_id=loss_set_group_id,
                    poll_interval=poll_interval,
                    timeout=timeout,
                ): i
                for i, q in enumerate(self._queued)
            }
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    results[i] = fut.result()
                except Exception as exc:
                    results[i] = LossSetResult(
                        name=self._queued[i].name,
                        status="error",
                        status_message=f"upload crashed: {exc!r}",
                    )

        aggregate = LossGroupUploadResult(
            loss_set_group_id=loss_set_group_id,
            loss_sets=list(results),
        )

        # 3) Attach (always, even if all errored -- so UW can at least see the group).
        try:
            attach_resp = session.post(
                f"{base}:{deal_port}/api/Programs/{self._program_id}/AttachLossGroup",
                json={
                    "lossSetGroupId": loss_set_group_id,
                    "name": self._name,
                    "description": None,
                },
            )
            attach_resp.raise_for_status()
            attach_body = attach_resp.json()
            aggregate.program_loss_group_id = int(attach_body["programLossGroupId"])
            aggregate.program_version_id = int(attach_body["programVersionId"])
        except Exception as exc:
            raise LossGroupAttachError(
                f"attach failed for loss group {loss_set_group_id}: {exc}",
                partial_result=aggregate,
            ) from exc

        return aggregate

    def _upload_one(
        self,
        queued: "_QueuedLossSet",
        loss_set_group_id: int,
        poll_interval: int,
        timeout: int,
    ) -> "LossSetResult":
        session = self._client._session
        base = self._client.base_url
        port = self._client.PORTS["loss"]

        def error_result(msg: str, *, loss_set_id=None, file_guid=None) -> "LossSetResult":
            return LossSetResult(
                name=queued.name,
                status="error",
                loss_set_id=loss_set_id,
                file_guid=file_guid,
                status_message=msg,
            )

        body = [self._build_loss_set_body(queued, loss_set_group_id)]
        try:
            post_resp = session.post(f"{base}:{port}/api/LossSets", json=body)
            post_resp.raise_for_status()
            post_body = post_resp.json()[0]
            loss_set_id = int(post_body["lossSet"]["id"])
            file_guid = str(post_body["file"]["fileGUID"])
        except Exception as exc:
            return error_result(f"POST /api/LossSets failed: {exc}")

        try:
            url_resp = session.post(
                f"{base}:{port}/api/LossSets/{file_guid}/PresignedUploadUrl"
            )
            url_resp.raise_for_status()
            presigned_url = str(url_resp.json()["url"])
        except Exception as exc:
            return error_result(
                f"PresignedUploadUrl failed: {exc}",
                loss_set_id=loss_set_id,
                file_guid=file_guid,
            )

        # Bypass the authenticated session for the S3 PUT — presigned URLs
        # reject the Foundation Authorization header.
        try:
            put_resp = requests.put(
                presigned_url,
                data=queued.csv_bytes,
                headers={"Content-Type": "text/csv"},
            )
            put_resp.raise_for_status()
        except Exception as exc:
            return error_result(
                f"S3 PUT failed: {exc}",
                loss_set_id=loss_set_id,
                file_guid=file_guid,
            )

        deadline = time.monotonic() + timeout
        last_info: dict = {}
        status_id = _STATUS_SUBMIT
        while True:
            try:
                info_resp = session.get(f"{base}:{port}/api/Files/FileByGuid/{file_guid}")
                info_resp.raise_for_status()
                last_info = info_resp.json() or {}
                status_id = int(last_info.get("uploadStatusId") or _STATUS_SUBMIT)
            except requests.HTTPError as exc:
                # 4xx is unrecoverable (file/route gone or wrong); stop polling immediately
                # so callers don't sit silently for the full timeout.
                if 400 <= exc.response.status_code < 500:
                    return error_result(
                        f"poll failed: {exc.response.status_code} {exc.response.reason}",
                        loss_set_id=loss_set_id,
                        file_guid=file_guid,
                    )
                last_info = {"statusMessage": f"poll error: {exc}"}
                status_id = _STATUS_SUBMIT
            except Exception as exc:
                last_info = {"statusMessage": f"poll error: {exc}"}
                status_id = _STATUS_SUBMIT

            if status_id == _STATUS_READY or status_id == _STATUS_ERROR:
                break
            if time.monotonic() >= deadline:
                return LossSetResult(
                    name=queued.name,
                    status="timeout",
                    loss_set_id=loss_set_id,
                    file_guid=file_guid,
                    status_message=str(last_info.get("statusMessage") or "poll timeout"),
                )
            if poll_interval > 0:
                time.sleep(poll_interval)

        return LossSetResult(
            name=queued.name,
            status="error" if status_id == _STATUS_ERROR else "success",
            loss_set_id=loss_set_id,
            file_guid=file_guid,
            row_count=int(last_info.get("rowCount") or 0),
            status_message=str(last_info.get("statusMessage") or ""),
        )

    @staticmethod
    def _build_loss_set_body(queued: "_QueuedLossSet", loss_set_group_id: int) -> Dict[str, Any]:
        loss_type_id = _YELT_LOSS_TYPE_ID if queued.loss_type == "yelt" else _ELT_LOSS_TYPE_ID
        return {
            "activeFlag": True,
            "lossTypeId": loss_type_id,
            "description": queued.name,
            "simYears": queued.sim_years,
            "currencyCode": queued.currency_code,
            "originalFileName": f"{queued.name}.csv",
            "sourceKey": str(loss_set_group_id),
            "lossSetEventSets": [{"eventSetId": queued.event_set_id}],
            "lossSetPerspectives": [{"perspectiveId": queued.perspective_id}],
            "lossSetVariants": [{"variantId": queued.variant_id}],
            "lossSetVendors": [{"vendorId": queued.vendor_id}],
            "nmpFlag": queued.nmp_flag,
            "nmpModelId": queued.nmp_model_id,
            "note": queued.note,
            "eventSplitViewId": queued.event_split_view_id,
        }

    # ---- internals ----

    def _add(
        self,
        loss_type: LossTypeStr,
        *,
        name: str,
        data: Any,
        currency_code: str,
        event_set_id,
        perspective_id,
        variant_id,
        vendor_id,
        sim_years: int,
        note,
        nmp_flag: bool,
        nmp_model_id,
        event_split_view_id,
        column_map,
    ) -> None:
        if not name or not name.strip():
            raise LossSetValidationError("loss set name is required")
        if not isinstance(sim_years, int) or sim_years <= 0:
            raise LossSetValidationError(
                f"sim_years must be a positive integer (got {sim_years!r}); "
                "required for both YELT and ELT"
            )

        sys_config = self._client.sys_config
        if not sys_config.is_loaded_for(loss_type):
            raise RuntimeError(
                f"SysConfig for {loss_type.upper()} not loaded; missing keys: "
                f"{sys_config.missing_keys(loss_type)}"
            )

        canonical = sys_config.columns_for(loss_type)
        csv_bytes = build_csv(data, canonical, column_map=column_map)

        resolved = _QueuedLossSet(
            name=name,
            loss_type=loss_type,
            csv_bytes=csv_bytes,
            currency_code=currency_code,
            event_set_id=self._resolve_id(event_set_id, "event_set", "get_event_set"),
            perspective_id=self._resolve_id(perspective_id, "perspective", "get_perspective"),
            variant_id=self._resolve_id(variant_id, "variant", "get_variant"),
            vendor_id=self._resolve_id(vendor_id, "vendor", "get_vendor"),
            sim_years=sim_years,
            note=note,
            nmp_flag=nmp_flag,
            nmp_model_id=nmp_model_id,
            event_split_view_id=event_split_view_id,
        )
        self._queued.append(resolved)

    def _resolve_id(self, value, label: str, getter_name: str) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            getter = getattr(self._client, getter_name)
            resolved = getter(value)
            if resolved is None:
                raise LossSetValidationError(
                    f"{label} '{value}' not found in reference data"
                )
            return int(resolved.id)
        raise LossSetValidationError(
            f"{label} must be int ID or string name (got {type(value).__name__})"
        )
