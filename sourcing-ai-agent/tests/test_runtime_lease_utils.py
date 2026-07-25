import unittest
from unittest import mock

from sourcing_agent.runtime_lease_utils import (
    worker_lease_owner_is_dead_local_process,
    workflow_job_lease_owner_is_dead_local_process,
)


class RuntimeLeaseUtilsTest(unittest.TestCase):
    def test_dead_local_worker_daemon_owner_is_reclaimable(self) -> None:
        with (
            mock.patch("sourcing_agent.runtime_lease_utils.socket.gethostname", return_value="local-host"),
            mock.patch("sourcing_agent.runtime_lease_utils.os.kill", side_effect=ProcessLookupError),
        ):
            self.assertTrue(
                worker_lease_owner_is_dead_local_process("worker-recovery-daemon-local-host-999999")
            )
            self.assertTrue(
                worker_lease_owner_is_dead_local_process("job-recovery-job123-local-host-999999")
            )
            self.assertTrue(
                worker_lease_owner_is_dead_local_process("recovery-daemon-local-host-999999")
            )

    def test_live_or_remote_owner_is_not_reclaimable(self) -> None:
        with (
            mock.patch("sourcing_agent.runtime_lease_utils.socket.gethostname", return_value="local-host"),
            mock.patch("sourcing_agent.runtime_lease_utils.os.kill", return_value=None),
        ):
            self.assertFalse(
                worker_lease_owner_is_dead_local_process("worker-recovery-daemon-local-host-123")
            )
        with mock.patch("sourcing_agent.runtime_lease_utils.socket.gethostname", return_value="local-host"):
            self.assertFalse(
                worker_lease_owner_is_dead_local_process("worker-recovery-daemon-remote-host-123")
            )
            self.assertFalse(worker_lease_owner_is_dead_local_process("manual-operator-123"))

    def test_dead_local_workflow_job_owner_is_reclaimable(self) -> None:
        with (
            mock.patch("sourcing_agent.runtime_lease_utils.socket.gethostname", return_value="local-host"),
            mock.patch("sourcing_agent.runtime_lease_utils.os.kill", side_effect=ProcessLookupError),
        ):
            self.assertTrue(workflow_job_lease_owner_is_dead_local_process("local-host:999999:111"))

    def test_live_or_remote_workflow_job_owner_is_not_reclaimable(self) -> None:
        with (
            mock.patch("sourcing_agent.runtime_lease_utils.socket.gethostname", return_value="local-host"),
            mock.patch("sourcing_agent.runtime_lease_utils.os.kill", return_value=None),
        ):
            self.assertFalse(workflow_job_lease_owner_is_dead_local_process("local-host:123:111"))
        with mock.patch("sourcing_agent.runtime_lease_utils.socket.gethostname", return_value="local-host"):
            self.assertFalse(workflow_job_lease_owner_is_dead_local_process("remote-host:999999:111"))
            self.assertFalse(workflow_job_lease_owner_is_dead_local_process("manual-operator-123"))


if __name__ == "__main__":
    unittest.main()
