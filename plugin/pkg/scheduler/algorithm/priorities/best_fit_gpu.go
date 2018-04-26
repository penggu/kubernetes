package priorities

import (
	"k8s.io/api/core/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
	"sort"
	"math"
	"k8s.io/kubernetes/plugin/pkg/scheduler/util"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// BestFitGpuPriorityMap is a priority function that favors nodes with the most requested individual GPU devices that meet t
// the given pod's requirements.
// It calculates the percentage of individual GPU device usage remaining after the pod's containers are placed on the most
// loaded GPUs possible, and prioritizes based on the sum of the least remaining.  This scheme considers the container
// placements individually, and so may make sub-optimal decisions in the case where a pod has many containers.
func BestFitGpuPriorityMap(pod *v1.Pod, meta interface{}, nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	zero := makeHostPriority(nodeInfo.Node().Name,0)

	// If the feature gate is disabled then ignore this priority function
	if !feature.DefaultFeatureGate.Enabled(features.MultiGPUScheduling) {
		return zero, nil
	}

	// If the pod doesn't request GPU then ignore this priority function
	if !podHasGpuRequest(pod) {
		return zero, nil
	}

	// Make a shallow copy of the individual gpu usage
	// We just use the copy() function because we don't care about the PodUsage map, just the
	// overall usage of the Gpu for the purpose of finding the best fit gpus
	GpuInfo := make([]schedulercache.NvidiaGPUInfo,len(nodeInfo.RequestedResource().NvidiaGPUInfoList))
	copy(GpuInfo,nodeInfo.RequestedResource().NvidiaGPUInfoList)

	return makeHostPriority(nodeInfo.Node().Name,
				computeScore4AllContainers(pod.Spec.Containers,GpuInfo)), nil
}

func podHasGpuRequest(pod *v1.Pod) bool {
	for _,c := range pod.Spec.Containers {
		for rName := range c.Resources.Requests {
			if rName == v1.NvidiaGPUScalarResourceName {
				return true
			}
		}
	}
	return false
}

// generates a HostPriority for the given node with the given score
func makeHostPriority(hostname string, score int) schedulerapi.HostPriority {
	return schedulerapi.HostPriority{
		Host:  hostname,
		Score: score,
	}
}

// Continually place containers on the most used GPU, going in order of greatest requested
// GPU amount, adding the resulting GPU usage amount to the total score.  Return the average
// score over all containers
func computeScore4AllContainers(containers []v1.Container, gpus []schedulercache.NvidiaGPUInfo) int {
	total := 0
	// Sort the containers in order of decreasing GPU request
	sort.Slice(containers, func (i, j int) bool{
		return util.HigherGpuRequestContainer(&containers[i],&containers[j])
	})
	for _,c := range containers {
		total += computeScore4SingleContainer(c,gpus)
	}
	return (total / len(containers))
}

// compute a score for placing a gpu allocation amount on the best fit gpus, placement should never
// fail as the node was pre-screened by the predicate stage
func computeScore4SingleContainer(container v1.Container, gpus []schedulercache.NvidiaGPUInfo) int {
	requested := util.GetContainerGpuRequest(&container)
	if requested == 0 {
		return 0
	}
	result := 0
	remaining := requested
	for remaining > 0 {
		// if > Max, place Max, otherwise place what's left
		to_place := remaining % v1.NvidiaGPUMaxUsage
		if remaining > v1.NvidiaGPUMaxUsage {
			to_place = v1.NvidiaGPUMaxUsage
		}

		// sort the list in decreasing order of availability
		sort.Slice(gpus,func(i, j int) bool {
			return (gpus[i].Usage > gpus[j].Usage)
		})
		// pick the first gpu off the list that can fit the requirement and add the placement score
		for i := range gpus {
			if v1.NvidiaGPUMaxUsage >= gpus[i].Usage + to_place {
				gpus[i].Usage += to_place
				result += int(math.Ceil(float64(gpus[i].Usage * schedulerapi.MaxPriority) / float64(v1.NvidiaGPUMaxUsage)))
				break
			}
		}
		remaining -= to_place
	}
	return result
}