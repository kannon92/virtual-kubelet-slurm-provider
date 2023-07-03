package slurm

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/virtual-kubelet/node-cli/manager"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	operatingSystem string = "Linux"
)

// Provider implements the virtual-kubelet provider interface and communicates with the Slurm REST API.
type Provider struct {
	resourceManager *manager.ResourceManager
	nodeName        string
	operatingSystem string
	nomadAddress    string
	nomadRegion     string
	cpu             string
	memory          string
	pods            string
}

// NewProvider creates a new Provider
func NewProvider(rm *manager.ResourceManager, nodeName string) (*Provider, error) {
	p := Provider{}
	return &p, nil
}

// CreatePod accepts a Pod definition and creates
// a Nomad job
func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("CreatePod %q\n", pod.Name)

	// Ignore daemonSet Pod
	if pod != nil && pod.OwnerReferences != nil && len(pod.OwnerReferences) != 0 && pod.OwnerReferences[0].Kind == "DaemonSet" {
		log.Printf("Skip to create DaemonSet pod %q\n", pod.Name)
		return nil
	}

	if pod != nil && len(pod.Spec.InitContainers) > 0 {
		return fmt.Errorf("Only support pod with 1 container. No Init containers")
	}

	if pod != nil && len(pod.Spec.Containers) > 1 {
		return fmt.Errorf("Only support pod with 1 container")
	}

	//

	return nil
}

// UpdatePod is a noop, nomad does not support live updates of a pod.
func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	log.Println("Pod Update called: No-op as not implemented")
	return nil
}

// DeletePod accepts a Pod definition and deletes a Nomad job.
func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) (err error) {
	// Deregister job
	return nil
}

// GetPod returns the pod running in the Nomad cluster. returns nil
// if pod is not found.
func (p *Provider) GetPod(ctx context.Context, namespace, name string) (pod *v1.Pod, err error) {
	// Get nomad job

	return nil, nil
}

// GetContainerLogs retrieves the logs of a container by name from the provider.
func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader("")), nil
}

// GetPodFullName as defined in the provider context
func (p *Provider) GetPodFullName(ctx context.Context, namespace string, pod string) string {
	return "NOTIMPLEMENTED"
}

// RunInContainer executes a command in a container in the pod, copying data
// between in/out/err and the container's stdin/stdout/stderr.
// TODO: Implementation
func (p *Provider) RunInContainer(ctx context.Context, namespace, name, container string, cmd []string, attach api.AttachIO) error {
	log.Printf("ExecInContainer %q\n", container)
	return nil
}

// GetPodStatus returns the status of a pod by name that is running as a job
// in the Nomad cluster returns nil if a pod by that name is not found.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

// GetPods returns a list of all pods known to be running in Nomad nodes.
func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	log.Printf("GetPods\n")

	return nil, nil
}

// Capacity returns a resource list containing the capacity limits set for Nomad.
func (p *Provider) Capacity(ctx context.Context) v1.ResourceList {
	// TODO: Use nomad /nodes api to get a list of nodes in the cluster
	// and then use the read node /node/:node_id endpoint to calculate
	// the total resources of the cluster to report back to kubernetes.
	return v1.ResourceList{
		"cpu":    resource.MustParse("20"),
		"memory": resource.MustParse("100Gi"),
		"pods":   resource.MustParse("20"),
	}
}

// NodeConditions returns a list of conditions (Ready, OutOfDisk, etc), for updates to the node status
// within Kubernetes.
func (p *Provider) NodeConditions(ctx context.Context) []v1.NodeCondition {
	// TODO: Make these dynamic.
	return []v1.NodeCondition{
		{
			Type:               "Ready",
			Status:             v1.ConditionTrue,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletReady",
			Message:            "kubelet is ready.",
		},
		{
			Type:               "OutOfDisk",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientDisk",
			Message:            "kubelet has sufficient disk space available",
		},
		{
			Type:               "MemoryPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
		{
			Type:               "NetworkUnavailable",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "RouteCreated",
			Message:            "RouteController created a route",
		},
	}

}

// NodeAddresses returns a list of addresses for the node status
// within Kubernetes.
func (p *Provider) NodeAddresses(ctx context.Context) []v1.NodeAddress {
	// TODO: Use nomad api to get a list of node addresses.
	return nil
}

func (p *Provider) ConfigureNode(ctx context.Context, n *v1.Node) {
	n.Status.Capacity = p.Capacity(ctx)
	n.Status.Conditions = p.NodeConditions(ctx)
	n.Status.Addresses = p.NodeAddresses(ctx)
	n.Status.DaemonEndpoints = *p.NodeDaemonEndpoints(ctx)
	n.Status.NodeInfo.OperatingSystem = p.operatingSystem
}

// NodeDaemonEndpoints returns NodeDaemonEndpoints for the node status
// within Kubernetes.
func (p *Provider) NodeDaemonEndpoints(ctx context.Context) *v1.NodeDaemonEndpoints {
	return &v1.NodeDaemonEndpoints{}
}

// OperatingSystem returns the operating system for this provider.
// This is a noop to default to Linux for now.
func (p *Provider) OperatingSystem() string {
	return operatingSystem
}
