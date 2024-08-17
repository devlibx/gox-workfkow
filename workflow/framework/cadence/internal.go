package cadence

import (
	"github.com/devlibx/gox-base/v2/errors"
)

func (c *Config) Validate() error {

	// Make sure we have worker groups
	if c.WorkerGroups == nil || len(c.WorkerGroups) == 0 {
		return errors.New("worker group is nil or empty")
	}

	// Make sure we do not duplicate task list names across all worker groups
	taskLists := map[string]string{}
	for _, wg := range c.WorkerGroups {
		for _, w := range wg.Workers {
			if _, ok := taskLists[w.TaskList]; ok {
				return errors.New("task list is duplicated = %s", w.TaskList)
			}
			taskLists[w.TaskList] = w.TaskList
		}
	}

	return nil
}

func (wg *WorkerGroup) Validate() error {
	if len(wg.Domain) == 0 {
		return errors.New("domain is empty for worker group = %s", wg.Name)
	}
	if len(wg.HostPort) == 0 {
		return errors.New("hostPort is empty for worker group = %s", wg.Name)
	}
	if wg.Workers == nil || len(wg.Workers) == 0 {
		return errors.New("workers is empty for worker group = %s", wg.Name)
	}
	for _, wg := range wg.Workers {
		if err := wg.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Worker) Validate() error {
	if len(s.TaskList) == 0 {
		return errors.New("TaskList is empty")
	}
	if s.WorkerCount < 0 {
		return errors.New("WorkerCount is less than 0")
	}
	return nil
}
