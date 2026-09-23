"""RollupRun: one run of a template, with its base and scenario jobs."""

import time
from typing import TYPE_CHECKING, Any

import requests

from ..exceptions import (
    FoundationHTTPError,
    NotFoundError,
    RateLimitError,
    RollupError,
    ServerError,
)
from ..utils import parse_utc_timestamp
from .collections import NamedCollection
from .portfolio import Portfolio
from .rows import Rows

if TYPE_CHECKING:
    from .foundation_client import FoundationClient

#: Common JobHistoryRunStatus names.
_JOB_STATUSES = {
    1: "Queued",
    2: "Processing",
    3: "Completed",
    4: "Error",
    5: "TimeOut",
    6: "Cancelled",
}
_COMPLETED = 3
_FINISHED_JOB = set(_JOB_STATUSES) - {1, 2}
#: A run's statuses after which it changes no more.
_FINISHED_RUN = {"Done", "Failed", "Cancelled"}


class RollupRun:
    """A template run: the request the platform queued, and the job of each
    portfolio it creates, the base portfolio's and one per scenario.

    Example:
        >>> run = tpl.run().wait()
        >>> run.jobs.as_polars()
        >>> p = run.portfolios[0]
    """

    def __init__(self, client: "FoundationClient", request_id: int):
        """Follow the run queued as ``request_id``; call refresh() to read it."""
        self._client = client
        self._request_id = request_id
        self._request: dict[str, Any] = {}
        self._jobs = Rows()
        self._portfolios: NamedCollection[Portfolio] | None = None

    @property
    def request_id(self) -> int:
        """The run's ID, as client.get_rollup_run takes it."""
        return self._request_id

    @property
    def template_id(self) -> int | None:
        """The saved template run; None for an unsaved template."""
        return self._request.get("templateId")

    @property
    def status(self) -> str | None:
        """Pending, Claimed, Done, Failed or Cancelled. Done means every job
        has finished, not that each succeeded; see ``succeeded``."""
        return self._request.get("status")

    @property
    def done(self) -> bool:
        """Whether the run has finished, one way or another."""
        return self.status in _FINISHED_RUN

    @property
    def succeeded(self) -> bool:
        """Whether the run is Done and every job completed."""
        return (
            self.status == "Done"
            and bool(self._jobs)
            and all(job["run_status"] == _COMPLETED for job in self._jobs)
        )

    @property
    def jobs(self) -> Rows:
        """The run's jobs as last read: the base portfolio's first, then each
        scenario's as the run creates them.

        Columns: job_id, kind ("base" or "scenario"), status, run_status,
        note, portfolio_id, run_start, run_end. ``note`` carries the job's
        progress while it runs and its errors after.
        """
        return self._jobs

    @property
    def portfolios(self) -> NamedCollection[Portfolio]:
        """The portfolios the run's completed jobs created.

        Raises:
            RuntimeError: If the run has not finished yet.
        """
        if not self.done:
            raise RuntimeError(
                f"Run {self._request_id} is {self.status}; its portfolios exist once it is done. "
                "Call wait() first."
            )
        if self._portfolios is None:
            self._portfolios = NamedCollection(
                self._client.get_portfolio(job["portfolio_id"])
                for job in self._jobs
                if job["run_status"] == _COMPLETED and job["portfolio_id"] is not None
            )
        return self._portfolios

    def refresh(self) -> "RollupRun":
        """Read the run and each of its jobs again.

        Returns:
            This run.
        """
        request = self._client._fetch(
            "portfolio", f"/app/Template/Run/{self._request_id}"
        )
        jobs = [("base", request.get("jobHistoryId"))]
        jobs += [
            ("scenario", job_id)
            for job_id in request.get("scenarioJobHistoryIds") or []
        ]
        self._jobs = Rows(
            _job_row(kind, self._client._fetch("common", f"/api/JobHistory/{job_id}"))
            for kind, job_id in jobs
            if job_id is not None
        )
        self._request = request
        return self

    def wait(
        self,
        *,
        timeout: float | None = 3600.0,
        poll_interval: float = 15.0,
        progress: bool = False,
    ) -> "RollupRun":
        """Poll until the run finishes, then check that every job completed.

        A server error, rate limit or network failure while polling is
        retried; any other error is raised at once. The run carries on after a timeout or
        Ctrl-C, and wait() can be called again.

        Args:
            timeout: Seconds to wait; None waits indefinitely.
            poll_interval: Seconds between polls.
            progress: Print a line whenever a job's status or note changes.

        Returns:
            This run, done, with every job completed.

        Raises:
            RollupError: If a job did not complete, the run failed or was
                cancelled, or it did not finish within ``timeout``.
            AuthenticationError: If the login expired; log in again and call
                client.get_rollup_run(run.request_id).
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        seen: dict[int, tuple[str, str | None]] = {}
        error: Exception | None = None
        while True:
            try:
                self.refresh()
            except (ServerError, RateLimitError) as exc:
                error = exc
            except FoundationHTTPError:
                raise
            except requests.RequestException as exc:
                error = exc
            else:
                error = None
                if progress:
                    self._report_progress(seen)
                if self.done:
                    break
            if error is not None and progress:
                print(f"poll failed, retrying: {error}")
            left = None if deadline is None else deadline - time.monotonic()
            if left is not None and left <= 0:
                failed = f" The last poll failed: {error}" if error else ""
                raise RollupError(
                    f"Run {self._request_id} did not finish within {timeout:g} s "
                    f"({self._describe_jobs()}). It carries on; call wait() again.{failed}",
                    run=self,
                )
            time.sleep(poll_interval if left is None else min(poll_interval, left))
        if not self.succeeded:
            raise RollupError(self._failure(), run=self)
        return self

    def cancel(self) -> list[int]:
        """Read the run again and ask the platform to stop each unfinished job.

        Returns:
            The IDs of unfinished jobs the platform could not find to stop:
            ones that finished meanwhile, or that another Portfolio instance
            runs. Empty when every stop was accepted.

        Raises:
            RollupError: If the run has created no job to stop yet.
        """
        self.refresh()
        if not self._jobs and not self.done:
            raise RollupError(
                f"Run {self._request_id} is {self.status} and has created no job to stop yet; "
                "call cancel() again shortly.",
                run=self,
            )
        not_found = []
        for job in self._jobs:
            if job["run_status"] not in _FINISHED_JOB:
                try:
                    self._client.request(
                        "portfolio", f"/api/jobs/{job['job_id']}", method="DELETE"
                    )
                except NotFoundError:
                    not_found.append(job["job_id"])
        return not_found

    def _report_progress(self, seen: dict[int, tuple[str, str | None]]) -> None:
        """Print each job whose status or note changed since ``seen``."""
        for job in self._jobs:
            state = (job["status"], job["note"])
            if seen.get(job["job_id"]) != state:
                seen[job["job_id"]] = state
                print(_job_line(job))

    def _failure(self) -> str:
        if not self._jobs:
            return f"Run {self._request_id} is {self.status} and created no job."
        lines = [f"Run {self._request_id} is {self.status}: {self._describe_jobs()}."]
        for job in self._jobs:
            if job["run_status"] not in _FINISHED_JOB:
                lines.append(f"  job {job['job_id']} ({job['kind']}) did not finish.")
            elif job["run_status"] != _COMPLETED:
                lines.append(f"  {_job_line(job)}")
        return "\n".join(lines)

    def _describe_jobs(self) -> str:
        return (
            ", ".join(f"job {j['job_id']} {j['status']}" for j in self._jobs)
            or "no job yet"
        )

    def describe(self) -> None:
        """Display a summary of this run.

        Example:
            >>> run.describe()
        """
        print("=" * 60)
        print(f"ROLLUP RUN {self._request_id}")
        print("=" * 60)
        print(f"  Template:  {self.template_id or '(unsaved template)'}")
        print(f"  Status:    {self.status}")
        for job in self._jobs:
            print(f"  {_job_line(job)}")
        print()
        print("METHODS:")
        print("  refresh()           -> RollupRun")
        print("  wait(timeout=3600)  -> RollupRun")
        print("  cancel()            -> list[int]")
        print("=" * 60)
        print()

    def __repr__(self) -> str:
        """String representation of the run."""
        return f"RollupRun(request_id={self._request_id}, status={self.status!r})"


def _job_line(job: dict[str, Any]) -> str:
    note = f", {job['note']}" if job["note"] else ""
    return f"job {job['job_id']} ({job['kind']}): {job['status']}{note}"


def _job_row(kind: str, job: dict[str, Any]) -> dict[str, Any]:
    external_ref = job.get("externalRef")
    return {
        "job_id": job["id"],
        "kind": kind,
        "status": _JOB_STATUSES.get(job.get("runStatus"), "Unknown"),
        "run_status": job.get("runStatus"),
        "note": job.get("note"),
        "portfolio_id": int(external_ref)
        if external_ref and external_ref.isdigit()
        else None,
        "run_start": parse_utc_timestamp(job.get("runStart")),
        "run_end": parse_utc_timestamp(job.get("runEnd")),
    }
