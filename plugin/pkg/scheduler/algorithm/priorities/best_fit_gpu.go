package priorities

import (
	"k8s.io/api/core/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
	"sort"
	"math"
	"k8s.io/apimachinery/pkg/api/resource"
)

// BestFitGpuPriorityMap is a priority function that favors nodes with the most requested individual GPU devices that meet t
// the given pod's requirements.
// It calculates the percentage of individual GPU device usage remaining after the pod's containers are placed on the most
// loaded GPUs possible, and prioritizes based on the sum of the least remaining.
func BestFitGpuPriorityMap(pod *v1.Pod, meta interface{}, nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	// If the pod doesn't request GPU then ignore this priority function
	if !podHasGpuRequest(pod) {
		return makeHostPriority(nodeInfo.Node().Name,0), nil
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
		var vali, valj resource.Quantity
		if _,ok := containers[i].Resources.Requests[v1.NvidiaGPUScalarResourceName]; ok {
			vali = containers[i].Resources.Requests[v1.NvidiaGPUScalarResourceName]
		}
		if _,ok := containers[j].Resources.Requests[v1.NvidiaGPUScalarResourceName]; ok {
			vali = containers[j].Resources.Requests[v1.NvidiaGPUScalarResourceName]
		}
		return ( vali.MilliValue() > valj.MilliValue() )
	})
	for _,c := range containers {
		amount := int64(0)
		if _,ok := c.Resources.Requests[v1.NvidiaGPUScalarResourceName]; ok {
			gpuQuant := c.Resources.Requests[v1.NvidiaGPUScalarResourceName]
			amount = gpuQuant.MilliValue()
		}
		total += computeScore4SingleContainer(amount,gpus)
	}
	return (total / len(containers))
}

// compute a score for placing a gpu allocation amount on the best fit gpus
func computeScore4SingleContainer(amount int64, gpus []schedulercache.NvidiaGPUInfo) int {
	if amount == 0 {
		return 0
	}
	// sort the list in decreasing order of availability
	sort.Slice(gpus,func(i, j int) bool {
		return gpus[i].Usage > gpus[j].Usage
	})
	// pick the first gpu off the list that can fit the requirement
	for _,g := range gpus {
		if v1.NvidiaGPUMaxUsage >= g.Usage + amount {
			g.Usage += amount
			return int(math.Ceil(float64(g.Usage * schedulerapi.MaxPriority) / float64(v1.NvidiaGPUMaxUsage)))
		}
	}
	return 0
}