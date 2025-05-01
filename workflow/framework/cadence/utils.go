package cadence

import (
	"context"
	"go.uber.org/cadence/.gen/go/shared"
)

// GetCadenceWorkflowStatus retrieves the status of a Cadence workflow execution.
// Parameters:
// - cadenceApi: An instance of the Cadence API to interact with the workflow service.
// - taskList: The task list associated with the workflow.
// - workflowId: The unique identifier of the workflow execution.
// - runId: The unique identifier of the workflow run.
// Returns:
// - A string representing the workflow status (e.g., "COMPLETED", "FAILED", "CANCELED", "TERMINATED", "CONTINUED_AS_NEW", "TIMED_OUT", "UNKNOWN", "NA").
// - A string representation of the workflow execution status details.
func GetCadenceWorkflowStatus(cadenceApi Api, taskList string, workflowId string, runId string) (string, string) {
	ret := "NA"
	status, err := cadenceApi.DescribeWorkflowExecution(ContextWithTaskListInfo(context.Background(), taskList), workflowId, runId)
	if err != nil {
		return ret, ""
	}
	switch status.WorkflowExecutionInfo.GetCloseStatus() {
	case shared.WorkflowExecutionCloseStatusCompleted:
		ret = "COMPLETED"
	case shared.WorkflowExecutionCloseStatusFailed:
		ret = "FAILED"
	case shared.WorkflowExecutionCloseStatusCanceled:
		ret = "CANCELED"
	case shared.WorkflowExecutionCloseStatusTerminated:
		ret = "TERMINATED"
	case shared.WorkflowExecutionCloseStatusContinuedAsNew:
		ret = "CONTINUED_AS_NEW"
	case shared.WorkflowExecutionCloseStatusTimedOut:
		ret = "TIMED_OUT"
	default:
		ret = "UNKNOWN"
	}
	return ret, status.String()
}
