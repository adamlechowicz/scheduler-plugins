/*
Copyright 2022 The Kubernetes Authors.

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

package topologicalsort

import (
	"context"
	"fmt"
	"bytes"
	"encoding/json"
	"net/http"
	"sync"

    "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/controller-runtime/pkg/client"

	pluginconfig "sigs.k8s.io/scheduler-plugins/apis/config"

	agv1alpha "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
)

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "TopologicalSort"
)

var scheme = runtime.NewScheme()
var pauseScheduling = false
var mu sync.Mutex

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agv1alpha.AddToScheme(scheme))
}

// TopologicalSort : Sort pods based on their AppGroup and corresponding microservice dependencies
type TopologicalSort struct {
	client.Client
	handle     framework.Handle
	namespaces []string
}

var _ framework.QueueSortPlugin = &TopologicalSort{}
var _ framework.PreFilterPlugin = &TopologicalSort{}

// Name : returns the name of the plugin.
func (ts *TopologicalSort) Name() string {
	return Name
}

// getArgs : returns the arguments for the TopologicalSort plugin.
func getArgs(obj runtime.Object) (*pluginconfig.TopologicalSortArgs, error) {
	TopologicalSortArgs, ok := obj.(*pluginconfig.TopologicalSortArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type TopologicalSortArgs, got %T", obj)
	}

	return TopologicalSortArgs, nil
}

// New : create an instance of a TopologicalSort plugin
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Creating new instance of the TopologicalSort plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	client, err := client.New(handle.KubeConfig(), client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}

	pl := &TopologicalSort{
		Client:     client,
		handle:     handle,
		namespaces: args.Namespaces,
	}
	return pl, nil
}

// gatherPodInfo gathers contextual information about the pods.
func gatherPodInfo(handle framework.Handle) ([]map[string]interface{}, error) {
    var podInfoList []map[string]interface{}
    pods, err := handle.SharedInformerFactory().Core().V1().Pods().Lister().Pods("spark-ns").List(labels.Everything())
    if err != nil {
        return nil, err
    }

    for _, pod := range pods {
		ownerReferences := make([]map[string]interface{}, len(pod.OwnerReferences))
        for i, ownerRef := range pod.OwnerReferences {
            ownerReferences[i] = map[string]interface{}{
                "name": ownerRef.Name,
            }
        }

        podInfo := map[string]interface{}{
            "name":      pod.Name,
            "namespace": pod.Namespace,
            "status":    pod.Status.Phase,
            "nodeName":  pod.Spec.NodeName,
			"ownerRefs": ownerReferences,
        }

		// // Include information about the most recently completed pod
        // if mostRecentCompletedPod != nil && pod.Name == mostRecentCompletedPod.Name && pod.Namespace == mostRecentCompletedPod.Namespace {
        //     podInfo["mostRecentCompleted"] = true
        // } else {
        //     podInfo["mostRecentCompleted"] = false
        // }
		
        podInfoList = append(podInfoList, podInfo)
    }
    return podInfoList, nil
}

// getPodOrderFromAPI makes an API call to get the order of pods.
func getPodOrderFromAPI(podInfoList []map[string]interface{}) ([]string, error) {
	// Check if podInfoList is empty
    if len(podInfoList) == 0 {
        return nil, nil
    }

    apiURL := "http://host.docker.internal:14040/pods"
    jsonData, err := json.Marshal(podInfoList)
    if err != nil {
        return nil, err
    }

    resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var podOrder []string
    if err := json.NewDecoder(resp.Body).Decode(&podOrder); err != nil {
        return nil, err
    }

	// Check for pause signal
    if len(podOrder) > 0 && podOrder[0] == "PAUSE" {
        mu.Lock()
        pauseScheduling = true
        mu.Unlock()
    } else {
        mu.Lock()
        pauseScheduling = false
        mu.Unlock()
    }

    return podOrder, nil
}

// Less is the function used by the activeQ heap algorithm to sort pods.
// 1) Sort Pods based on their AppGroup and corresponding service topology graph.
// 2) Otherwise, follow the strategy of the in-tree QueueSort Plugin (PrioritySort Plugin)
func (ts *TopologicalSort) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	podInfoList, err := gatherPodInfo(ts.handle)
    if err != nil {
        klog.ErrorS(err, "Failed to gather pod information")
        return false
    }

    podOrder, err := getPodOrderFromAPI(podInfoList)
    if err != nil {
        klog.ErrorS(err, "Failed to get pod order from API")
        return false
    }
	// log the pod order
	// klog.InfoS("Pod order", "order", podOrder)

	podOrderMap := make(map[string]int)
    for i, podName := range podOrder {
        podOrderMap[podName] = i
    }

    order1, ok1 := podOrderMap[pInfo1.Pod.Name]
    order2, ok2 := podOrderMap[pInfo2.Pod.Name]

    if !ok1 || !ok2 {
        return false
    }

    return order1 < order2

	// p1AppGroup := networkawareutil.GetPodAppGroupLabel(pInfo1.Pod)
	// p2AppGroup := networkawareutil.GetPodAppGroupLabel(pInfo2.Pod)
	// ctx := context.TODO()
	// logger := klog.FromContext(ctx)

	// // If pods do not belong to an AppGroup, or being to different AppGroups, follow vanilla QoS Sort
	// if p1AppGroup != p2AppGroup || len(p1AppGroup) == 0 {
	// 	logger.V(4).Info("Pods do not belong to the same AppGroup CR", "p1AppGroup", p1AppGroup, "p2AppGroup", p2AppGroup)
	// 	s := &queuesort.PrioritySort{}
	// 	return s.Less(pInfo1, pInfo2)
	// }

	// // Pods belong to the same appGroup, get the CR
	// logger.V(6).Info("Pods belong to the same AppGroup CR", "p1 name", pInfo1.Pod.Name, "p2 name", pInfo2.Pod.Name, "appGroup", p1AppGroup)
	// agName := p1AppGroup
	// appGroup := ts.findAppGroupTopologicalSort(ctx, logger, agName)

	// // Get labels from both pods
	// labelsP1 := pInfo1.Pod.GetLabels()
	// labelsP2 := pInfo2.Pod.GetLabels()

	// // Binary search to find both order index since topology list is ordered by Workload Name
	// orderP1 := networkawareutil.FindPodOrder(appGroup.Status.TopologyOrder, labelsP1[agv1alpha.AppGroupSelectorLabel])
	// orderP2 := networkawareutil.FindPodOrder(appGroup.Status.TopologyOrder, labelsP2[agv1alpha.AppGroupSelectorLabel])

	// logger.V(6).Info("Pod order values", "p1 order", orderP1, "p2 order", orderP2)

	// // Lower is better
	// return orderP1 <= orderP2
}

func (ts *TopologicalSort) findAppGroupTopologicalSort(ctx context.Context, logger klog.Logger, agName string) *agv1alpha.AppGroup {
	for _, namespace := range ts.namespaces {
		logger.V(6).Info("appGroup CR", "namespace", namespace, "name", agName)
		// AppGroup couldn't be placed in several namespaces simultaneously
		appGroup := &agv1alpha.AppGroup{}
		err := ts.Get(ctx, client.ObjectKey{
			Namespace: namespace,
			Name:      agName,
		}, appGroup)
		if err != nil {
			logger.V(4).Info("Cannot get AppGroup from AppGroupNamespaceLister:", "error", err)
			continue
		}
		if appGroup != nil {
			return appGroup
		}
	}
	return nil
}

// PreFilter is called at the beginning of the scheduling cycle.
func (ts *TopologicalSort) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
    mu.Lock()
    defer mu.Unlock()
    if pauseScheduling {
        return nil, framework.NewStatus(framework.Unschedulable, "Scheduling is paused")
    }
    return nil, framework.NewStatus(framework.Success, "")
}

// PreFilterExtensions returns nil because we do not implement any extensions.
func (ts *TopologicalSort) PreFilterExtensions() framework.PreFilterExtensions {
    return nil
}