/*
Copyright 2019 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package enqueue

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (enqueue *Action) Name() string {
	return "enqueue"
}

func (enqueue *Action) Initialize() {}

func (enqueue *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Enqueue ...")
	defer klog.V(5).Infof("Leaving Enqueue ...")

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueSet := sets.NewString()
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		if job.ScheduleStartTimestamp.IsZero() {
			ssn.Jobs[job.UID].ScheduleStartTimestamp = metav1.Time{
				Time: time.Now(),
			}
		}
		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if !queueSet.Has(string(queue.UID)) {
			klog.V(5).Infof("Added Queue <%s> for Job <%s/%s>",
				queue.Name, job.Namespace, job.Name)

			queueSet.Insert(string(queue.UID))
			queues.Push(queue)
		}

		if job.IsPending() {
			if _, found := jobsMap[job.Queue]; !found {
				jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			klog.V(5).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
			jobsMap[job.Queue].Push(job)
		}
	}

	klog.V(3).Infof("Try to enqueue PodGroup to %d Queues", len(jobsMap))

	// enqueuedJobs collects every job transitioned Pending→Inqueue in this cycle.
	// After the loop we flush their status to the apiserver immediately so that
	// job controllers (vc-controller-manager, PyTorchJob operator, …) can react
	// without waiting for CloseSession to finish the remaining actions.
	var enqueuedJobs []*api.JobInfo

	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)

		// skip the Queue that has no pending job
		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		}
		job := jobs.Pop().(*api.JobInfo)

		if job.PodGroup.Spec.MinResources == nil || ssn.JobEnqueueable(job) {
			ssn.JobEnqueued(job)
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			ssn.Jobs[job.UID] = job
			enqueuedJobs = append(enqueuedJobs, job)
		}

		// Added Queue back until no job in Queue.
		queues.Push(queue)
	}

	// Flush Inqueue status to the apiserver for every job that was just enqueued.
	// FlushPodGroupStatus calls only StatusUpdater.UpdatePodGroup; it deliberately
	// does NOT call RecordJobStatusEvent, preventing premature Unschedulable events
	// and pod-condition patches that belong to the CloseSession reconciliation path.
	for _, job := range enqueuedJobs {
		pg, err := ssn.FlushPodGroupStatus(job)
		if err != nil {
			klog.Errorf("Failed to flush PodGroup <%s/%s> status to Inqueue: %v",
				job.Namespace, job.Name, err)
			continue
		}
		// Keep the in-session snapshot consistent with what the apiserver accepted.
		job.PodGroup = pg
	}
}

func (enqueue *Action) UnInitialize() {}
