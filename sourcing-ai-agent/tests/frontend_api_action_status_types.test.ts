import { SourcingAgentApiClient } from "../contracts/frontend_api_adapter";
import {
  approveOperationAction,
  rejectOperationAction,
} from "../frontend-demo/src/lib/api";

declare const client: SourcingAgentApiClient;

async function assertActionSpecificStatusTypes(): Promise<void> {
  const submitted: "queued" | "approval_required" = (
    await client.submitOperationAction({})
  ).status;
  const queried: "ok" = (await client.getOperationAction("action")).status;
  const approved: "queued" = (await client.approveOperationAction("action")).status;
  const rejected: "rejected" = (await client.rejectOperationAction("action")).status;

  const cancelledRun: "cancelled" = (await client.cancelOperationRun("run")).status;
  const retriedRun: "queued" = (await client.retryOperationRun("run")).status;
  const resumedRun: "queued" = (await client.resumeOperationRun("run")).status;
  const dispatchedRun: "planned" = (await client.dispatchOperationRun("run")).status;

  const cancelledCommand: "cancelled" = (
    await client.cancelWorkflowCommand("command")
  ).status;
  const retriedCommand: "queued" = (await client.retryWorkflowCommand("command")).status;
  const resumedCommand: "queued" = (await client.resumeWorkflowCommand("command")).status;

  const demoApproved: "queued" = (await approveOperationAction("action")).status;
  const demoRejected: "rejected" = (await rejectOperationAction("action")).status;

  void submitted;
  void queried;
  void approved;
  void rejected;
  void cancelledRun;
  void retriedRun;
  void resumedRun;
  void dispatchedRun;
  void cancelledCommand;
  void retriedCommand;
  void resumedCommand;
  void demoApproved;
  void demoRejected;

  // @ts-expect-error approve cannot produce the reject outcome.
  const impossibleApprove: "rejected" = (await client.approveOperationAction("action")).status;
  // @ts-expect-error demo reject cannot produce the approve outcome.
  const impossibleDemoReject: "queued" = (await rejectOperationAction("action")).status;
  void impossibleApprove;
  void impossibleDemoReject;
}

void assertActionSpecificStatusTypes;
