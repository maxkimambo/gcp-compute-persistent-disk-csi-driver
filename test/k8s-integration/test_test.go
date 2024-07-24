/*
TODO: Fill out this section

Make sure to be authenticated with gcloud to the project that is running?
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1alpha1 "k8s.io/api/storage/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	k8sv1alpha1 "k8s.io/client-go/kubernetes/typed/storage/v1alpha1"
	"k8s.io/client-go/tools/clientcmd"

	computev1 "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"google.golang.org/api/option"
	// "k8s.io/client-go/util/retry"
	// "k8s.io/klog/v2"
)

const (
	defaultNamespace = "default"
	driverName       = "pd.csi.storage.gke.io"

	disksRequest   = "https://compute.googleapis.com/compute/v1/projects/%s/aggregated/disks"
	projectRequest = "https://metadata.google.internal/computeMetadata/v1/project/project-id"
	zoneRequest    = "https://metadata.google.internal/computeMetadata/v1/instance/zone"
)

// DiskInfo struct used for unmarshalling
type DiskInfo struct {
	projectName string
	pvName      string
	zone        string
}

type Disk struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Zone string `json:"zone"`
}

type DiskList struct {
	Disks []Disk `json:"disks"`
}

type AggregatedList struct {
	Items map[string]DiskList `json:"items"`
}

func TestControllerModifyVolume(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Run ControllerModifyVolume tests")
}

var _ = Describe("ControllerModifyVolume tests", func() {
	var (
		credsPath      string
		kubeConfigPath string
		projectName    string

		ctx       context.Context
		clientset *kubernetes.Clientset
		// computeClient *computev1.DisksClient
		storageClient *k8sv1alpha1.StorageV1alpha1Client
	)

	BeforeEach(func() {
		ctx = context.Background()
		kubeConfigPath = os.Getenv("KUBECONFIG")
		// If $KUBECONFIG is not set, we take the config from ~/.kube/config
		if kubeConfigPath == "" {
			kubeConfigPath = os.Getenv("HOME") + "/.kube/config"
		}

		projectName = strings.Trim(os.Getenv("PROJECT"), "\n")
		Expect(projectName).ToNot(Equal(""))

		credsPath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
		filepath.Clean(credsPath)

		// Setup clients
		config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		clientset, err = kubernetes.NewForConfig(config)
		Expect(err).To(BeNil())
		storageClient, err = k8sv1alpha1.NewForConfig(config)
		Expect(err).To(BeNil())

		// TODO: Read from the OS env instead
		// credentialsOption := option.WithCredentialsFile(credsPath)
		// computeClient, err = computev1.NewDisksRESTClient(ctx, credentialsOption)
		// Expect(err).To(BeNil())
		// defer computeClient.Close()
	})

	Context("Updates to hyperdisks", func() {
		It("HdB should pass with normal constraints", func() {
			credentialsOption := option.WithCredentialsFile(credsPath)
			computeClient, err2 := computev1.NewDisksRESTClient(ctx, credentialsOption)
			Expect(err2).To(BeNil())
			defer computeClient.Close()

			// TODO: change these to be constants
			initialSize := "100Gi"
			initialIops := "3000"
			initialThroughput := "150"
			storageClassName := "test-storageclass"

			err := createStorageClass(clientset, storageClassName, "hyperdisk-balanced", &initialIops, &initialThroughput, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("Made it after creating the StorageClass %s\n", storageClassName)

			vacName1 := "test-vac1"
			err = createVac(storageClient, vacName1, &initialIops, &initialThroughput, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("Made it after creating the VolumeAttributesClass %s\n", vacName1)

			pvcName := "test-pvc"
			err = createPvc(clientset, pvcName, initialSize, storageClassName, vacName1, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("Made it after creating the PersistentVolumeClaim %s\n", pvcName)

			podName := "test-pod"
			err = createPod(clientset, podName, pvcName, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("Made it after creating the Pod %s\n", podName)

			pvName, zoneName, err := getPVNameAndZone(clientset, computeClient, projectName, defaultNamespace, pvcName, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("The PV name is %s in zone %s\n", pvName, zoneName)

			diskInfo := DiskInfo{
				pvName:      pvName,
				projectName: projectName,
				zone:        zoneName,
			}
			iops, throughput, err := getMetadataFromPV(computeClient, diskInfo, true, true, ctx)
			Expect(strconv.FormatInt(iops, 10)).To(Equal(initialIops))
			Expect(strconv.FormatInt(throughput, 10)).To(Equal(initialThroughput))

			vacName2 := "test-vac2"
			updatedIops := "3013"
			updatedThroughput := "181"
			err = createVac(storageClient, vacName2, &updatedIops, &updatedThroughput, ctx)
			Expect(err).To(BeNil())

			err = patchPvc(clientset, pvcName, vacName2, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("The code made it past patching the pv!")

			err = waitUntilUpdate(computeClient, diskInfo, &iops, &throughput, ctx)
			Expect(err).To(BeNil())
			fmt.Printf("The PV updated the metadata!")

			iops, throughput, err = getMetadataFromPV(computeClient, diskInfo, true, true, ctx)
			Expect(strconv.FormatInt(iops, 10)).To(Equal(updatedIops))
			Expect(strconv.FormatInt(throughput, 10)).To(Equal(updatedThroughput))
		})
	})
})

func createStorageClass(clientset *kubernetes.Clientset, storageClassName string, diskType string, iops *string, throughput *string, ctx context.Context) error {
	waitForFirstConsumer := storagev1.VolumeBindingWaitForFirstConsumer
	parameters := map[string]string{
		"type": diskType,
	}
	if iops != nil {
		parameters["provisioned-iops-on-create"] = *iops
	}
	if throughput != nil {
		parameters["provisioned-throughput-on-create"] = *throughput + "Mi"
	}
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: storageClassName,
		},
		Provisioner:       driverName,
		Parameters:        parameters,
		VolumeBindingMode: &waitForFirstConsumer,
	}
	_, err := clientset.StorageV1().StorageClasses().Create(ctx, storageClass, metav1.CreateOptions{})
	return err
}

func createVac(storageClient *k8sv1alpha1.StorageV1alpha1Client, vacName string, iops *string, throughput *string, ctx context.Context) error {
	parameters := map[string]string{}
	if iops != nil {
		parameters["iops"] = *iops
	}
	if throughput != nil {
		parameters["throughput"] = *throughput
	}
	vac1 := &storagev1alpha1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: vacName,
		},
		DriverName: driverName,
		Parameters: parameters,
	}
	_, err := storageClient.VolumeAttributesClasses().Create(ctx, vac1, metav1.CreateOptions{})
	return err
}

func createPvc(clientset *kubernetes.Clientset, pvcName string, size string, storageClassName string, vacName string, ctx context.Context) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvcName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName:          &storageClassName,
			VolumeAttributesClassName: &vacName,
		},
	}
	_, err := clientset.CoreV1().PersistentVolumeClaims(defaultNamespace).Create(ctx, pvc, metav1.CreateOptions{})
	return err
}

func createPod(clientset *kubernetes.Clientset, podName string, pvcName string, ctx context.Context) error {
	pvcVolumeSource := corev1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcName,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "test-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &pvcVolumeSource,
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "nginx:1.14.2",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "test-vol",
							MountPath: "/vol",
						},
					},
				},
			},
		},
	}
	_, err := clientset.CoreV1().Pods(defaultNamespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func getPVNameAndZone(clientset *kubernetes.Clientset, computeClient *computev1.DisksClient, projectName string, nsName string, pvcName string, ctx context.Context) (string, string, error) {
	pvName, zones, err := getPVNameAndZones(clientset, nsName, pvcName, ctx)
	if err != nil {
		return "", "", err
	}
	fmt.Printf("The pvName is %s and the projectName is %s\n", pvName, projectName)
	getDiskRequest := &computepb.GetDiskRequest{
		Disk:    pvName,
		Project: projectName,
	}
	// zones represents all possible zones the pv is in, iterate to see which zone the PV is in
	for _, zone := range zones {
		fmt.Printf("Testing zone %s\n", zone)
		getDiskRequest.Zone = zone
		pv, err := computeClient.Get(ctx, getDiskRequest)
		if err == nil && pv != nil {
			return pvName, zone, nil
		}
	}
	return "", "", fmt.Errorf("Could not find the zone for the PV!")
}

// getPVNameAndZones returns the PV name and possible zones (based off the node affinity) corresponding to the pvcName in namespace nsName.
func getPVNameAndZones(clientset *kubernetes.Clientset, nsName string, pvcName string, ctx context.Context) (string, []string, error) {
	pvName := ""
	pvcErr := wait.PollUntilContextCancel(ctx, 60*time.Second, false, func(ctx context.Context) (bool, error) {
		pvc, err := clientset.CoreV1().PersistentVolumeClaims(nsName).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			return false, fmt.Errorf("PVC %s is not bound yet.", pvcName)
		}
		if pvc.Spec.VolumeName == "" {
			return false, fmt.Errorf("Could not get the PV name for the PVC %s.", pvcName)
		}
		pvName = pvc.Spec.VolumeName
		return true, nil
	})
	if pvcErr != nil {
		return "", nil, pvcErr
	}
	var zoneNames []string
	pvErr := wait.PollUntilContextCancel(ctx, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		pv, err := clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// TODO: find a way to clean this up
		if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil && pv.Spec.NodeAffinity.Required.NodeSelectorTerms != nil {
			if len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) > 0 {
				for _, nodeSelectorTerm := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
					for _, nodeSelectorRequirement := range nodeSelectorTerm.MatchExpressions {
						if nodeSelectorRequirement.Key == "topology.gke.io/zone" {
							if len(nodeSelectorRequirement.Values) > 0 {
								zoneNames = nodeSelectorRequirement.Values
								return true, nil
							}
						}
					}
				}
			}
		}
		return true, nil
	})
	if pvErr != nil {
		return "", nil, pvErr
	}
	return pvName, zoneNames, nil
}

func getMetadataFromPV(computeClient *computev1.DisksClient, diskInfo DiskInfo, getIops bool, getThroughput bool, ctx context.Context) (int64, int64, error) {
	getDiskRequest := &computepb.GetDiskRequest{
		Disk:    diskInfo.pvName,
		Project: diskInfo.projectName,
		Zone:    diskInfo.zone,
	}
	pv, err := computeClient.Get(ctx, getDiskRequest)
	var iops int64
	var throughput int64
	if err != nil {
		return iops, throughput, err
	}
	if getIops {
		iops = *pv.ProvisionedIops
	}
	if getThroughput {
		throughput = *pv.ProvisionedThroughput
	}
	return iops, throughput, nil
}

func patchPvc(clientset *kubernetes.Clientset, pvcName string, vacName string, ctx context.Context) error {
	patch := []map[string]interface{}{
		{
			"op":    "replace",
			"path":  "/spec/volumeAttributesClassName",
			"value": vacName,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = clientset.CoreV1().PersistentVolumeClaims(defaultNamespace).Patch(ctx, pvcName, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	return err
}

func waitUntilUpdate(computeClient *computev1.DisksClient, diskInfo DiskInfo, initialIops *int64, initialThroughput *int64, ctx context.Context) error {
	backoff := wait.Backoff{
		Duration: 1 * time.Minute,
		Factor:   1.0,
		Steps:    10,
		Cap:      11 * time.Minute,
	}
	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		iops, throughput, err := getMetadataFromPV(computeClient, diskInfo, initialIops != nil, initialThroughput != nil, ctx)
		if err != nil {
			return false, err
		}
		if initialIops != nil && *initialIops != iops {
			return true, nil
		}
		if initialThroughput != nil && *initialThroughput != throughput {
			return true, nil
		}
		return false, nil
	})
	return err
}
