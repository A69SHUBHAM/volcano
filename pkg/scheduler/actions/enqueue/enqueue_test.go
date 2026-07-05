/*
Copyright 2024 The Volcano Authors.

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
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"
	nodeshardv1alpha1 "volcano.sh/apis/pkg/apis/shard/v1alpha1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/drf"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/plugins/sla"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// trackingStatusUpdater is a StatusUpdater that counts UpdatePodGroup calls.
type trackingStatusUpdater struct {
	updatePodGroupCalls atomic.Int32
}

func (t *trackingStatusUpdater) UpdatePodStatus(pod *v1.Pod) (*v1.Pod, error) {
	return pod, nil
}

func (t *trackingStatusUpdater) UpdatePodGroup(pg *api.PodGroup) (*api.PodGroup, error) {
	t.updatePodGroupCalls.Add(1)
	return pg, nil
}

func (t *trackingStatusUpdater) UpdateQueueStatus(_ *api.QueueInfo) error {
	return nil
}

func (t *trackingStatusUpdater) UpdateNodeShardStatus(ns *nodeshardv1alpha1.NodeShard) (*nodeshardv1alpha1.NodeShard, error) {
	return ns, nil
}

func TestEnqueue(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		drf.PluginName:        drf.New,
		gang.PluginName:       gang.New,
		sla.PluginName:        sla.New,
		proportion.PluginName: proportion.New,
	}
	options.Default()
	tests := []uthelper.TestCommonStruct{
		{
			Name: "when podgroup status is inqueue",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "c1", 2, nil, schedulingv1.PodGroupInqueue),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("c1", 1, api.BuildResourceList("4", "4G")),
				util.BuildQueue("c2", 1, api.BuildResourceList("4", "4G")),
			},
			ExpectStatus: map[api.JobID]scheduling.PodGroupPhase{
				"c1/pg1": scheduling.PodGroupInqueue,
			},
		},
		{
			Name: "when podgroup status is pending",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "c1", 1, nil, schedulingv1.PodGroupPending),
				util.BuildPodGroup("pg2", "c1", "c2", 1, nil, schedulingv1.PodGroupPending),
			},
			Pods: []*v1.Pod{
				// pending pod with owner1, under ns:c1/q:c1
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("3", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				// pending pod with owner2, under ns:c1/q:c2
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("c1", 1, api.BuildResourceList("4", "4G")),
				util.BuildQueue("c2", 1, api.BuildResourceList("4", "4G")),
			},
			ExpectStatus: map[api.JobID]scheduling.PodGroupPhase{
				"c1/pg1": scheduling.PodGroupInqueue,
				"c1/pg2": scheduling.PodGroupInqueue,
			},
		},
		{
			Name: "when podgroup status is running",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "c1", 2, nil, schedulingv1.PodGroupRunning),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "p1", "", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "p2", "", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("c1", 1, api.BuildResourceList("4", "4G")),
			},
			ExpectStatus: map[api.JobID]scheduling.PodGroupPhase{
				"c1/pg1": scheduling.PodGroupRunning,
			},
		},
		{
			Name: "pggroup cannot enqueue because the specified queue is c1, but there is only c2",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroup("pg1", "c1", "c1", 0, nil, schedulingv1.PodGroupPending),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("c2", 1, api.BuildResourceList("4", "4G")),
			},
			ExpectStatus: map[api.JobID]scheduling.PodGroupPhase{},
		},
		{
			Name: "pggroup cannot enqueue because queue resources are less than podgroup MinResources",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithMinResources("pg1", "c1", "c1", 1,
					nil, api.BuildResourceList("8", "8G"), schedulingv1.PodGroupPending),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("c1", 1, api.BuildResourceList("1", "1G")),
			},
			ExpectStatus: map[api.JobID]scheduling.PodGroupPhase{
				"c1/pg1": scheduling.PodGroupPending,
			},
		},
	}

	trueValue := true
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:               drf.PluginName,
					EnabledJobOrder:    &trueValue,
					EnabledJobEnqueued: &trueValue,
				},
				{
					Name:               proportion.PluginName,
					EnabledQueueOrder:  &trueValue,
					EnabledJobEnqueued: &trueValue,
				},
				{
					Name: sla.PluginName,
					Arguments: map[string]interface{}{
						"sla-waiting-time": "3m",
					},
					EnabledJobOrder:    &trueValue,
					EnabledJobEnqueued: &trueValue,
				},
				{
					Name:            gang.PluginName,
					EnabledJobOrder: &trueValue,
				},
			},
		},
	}
	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test.Plugins = plugins
			test.RegisterSession(tiers, nil)
			defer test.Close()
			action := New()
			test.Run([]framework.Action{action})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestEnqueueImmediateFlush verifies that every PodGroup transitioned to Inqueue during
// the enqueue action is flushed to the apiserver (via FlushPodGroupStatus→UpdatePodGroup)
// immediately at the end of Execute, before CloseSession runs, and that only the
// PodGroup status write is triggered — no Unschedulable events, no pod-condition patches.
func TestEnqueueImmediateFlush(t *testing.T) {
	options.Default()
	framework.RegisterPluginBuilder(drf.PluginName, drf.New)
	framework.RegisterPluginBuilder(gang.PluginName, gang.New)
	framework.RegisterPluginBuilder(sla.PluginName, sla.New)
	framework.RegisterPluginBuilder(proportion.PluginName, proportion.New)
	defer framework.CleanupPluginBuilders()

	trueValue := true
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:               drf.PluginName,
					EnabledJobOrder:    &trueValue,
					EnabledJobEnqueued: &trueValue,
				},
				{
					Name:               proportion.PluginName,
					EnabledQueueOrder:  &trueValue,
					EnabledJobEnqueued: &trueValue,
				},
				{
					Name: sla.PluginName,
					Arguments: map[string]interface{}{
						"sla-waiting-time": "3m",
					},
					EnabledJobOrder:    &trueValue,
					EnabledJobEnqueued: &trueValue,
				},
				{
					Name:            gang.PluginName,
					EnabledJobOrder: &trueValue,
				},
			},
		},
	}

	// Two pending PodGroups, both eligible for enqueue.
	podGroups := []*schedulingv1.PodGroup{
		util.BuildPodGroup("pg1", "c1", "c1", 1, nil, schedulingv1.PodGroupPending),
		util.BuildPodGroup("pg2", "c1", "c1", 1, nil, schedulingv1.PodGroupPending),
	}
	pods := []*v1.Pod{
		util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
		util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
	}
	queues := []*schedulingv1.Queue{
		util.BuildQueue("c1", 1, api.BuildResourceList("8", "8G")),
	}

	tracker := &trackingStatusUpdater{}
	binder := util.NewFakeBinder(0)
	evictor := util.NewFakeEvictor(0)
	stop := make(chan struct{})
	defer close(stop)

	schedulerCache := cache.NewCustomMockSchedulerCache("utmock-scheduler", binder, evictor, tracker, nil, nil)
	schedulerCache.Run(stop)
	schedulerCache.WaitForCacheSync(stop)

	for _, pod := range pods {
		schedulerCache.AddPod(pod)
	}
	for _, pg := range podGroups {
		schedulerCache.AddPodGroupV1beta1(pg)
	}
	for _, q := range queues {
		schedulerCache.AddQueueV1beta1(q)
	}

	ssn := framework.OpenSession(schedulerCache, tiers, nil)
	defer framework.CloseSession(ssn)

	action := New()
	action.Initialize()
	action.Execute(ssn)
	action.UnInitialize()

	// After Execute returns (and before CloseSession), the Inqueue status must
	// already have been flushed to the apiserver for every newly enqueued job.
	got := int(tracker.updatePodGroupCalls.Load())
	if got != 2 {
		t.Errorf("expected UpdatePodGroup to be called 2 times during Execute (one per enqueued job), got %d", got)
	}
}
