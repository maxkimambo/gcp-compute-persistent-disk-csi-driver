/*
TODO: Fill out this section

Make sure to be authenticated with gcloud to the project that is running?
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storagev1alpha1 "k8s.io/api/storage/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	driverName = "pd.csi.storage.gke.io"

	disksRequest   = "https://compute.googleapis.com/compute/v1/projects/%s/aggregated/disks"
	projectRequest = "https://metadata.google.internal/computeMetadata/v1/project/project-id"
	zoneRequest    = "https://metadata.google.internal/computeMetadata/v1/instance/zone"
)

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

func TestApplyVac(t *testing.T) {
	ctx := context.Background()

	kubeConfigPath := os.Getenv("KUBECONFIG")
	fmt.Printf("The os env for KUBECONFIG is: %s\n", kubeConfigPath)
	if kubeConfigPath == "" {
		kubeConfigPath = os.Getenv("HOME") + "/.kube/config"
	}
	fmt.Printf("kubeConfig path is: %s\n", kubeConfigPath)

	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	// TODO: change these to be much better
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if config == nil {
		t.Fatalf("Error: cannot get config")
	}
	// TODO: see if these can be combined to avoid having two clients
	// Create a clientset for kubernetes and kubernetes alpha (to access VolumeAttributesClasses)
	clientset, err := kubernetes.NewForConfig(config)
	cs, k8sErr := k8sv1alpha1.NewForConfig(config)
	if err != nil || k8sErr != nil {
		t.Fatalf("Error: cannot create clientset")
	}
	// TODO: figure out if creating ns is necessary or if it just complicates things
	// TODO:
	nsName := "test-ns"
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsName,
		},
	}
	_, nsErr := clientset.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
	if nsErr != nil {
		t.Fatalf("Could not create namespace %s: %v", nsName, nsErr)
	}
	fmt.Printf("Made it after creating the namespace %s\n", nsName)

	// TODO: change these to be constants
	initialSize := "100Gi"
	initialIops := "3000"
	initialThroughput := "150"
	storageClassName := "test-storageclass"
	waitForFirstConsumer := storagev1.VolumeBindingWaitForFirstConsumer
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsName,
			Name:      storageClassName,
		},
		Provisioner: driverName,
		Parameters: map[string]string{
			"type":                             "hyperdisk-balanced",
			"provisioned-iops-on-create":       initialIops,
			"provisioned-throughput-on-create": initialThroughput + "Mi",
		},
		VolumeBindingMode: &waitForFirstConsumer,
	}
	_, scErr := clientset.StorageV1().StorageClasses().Create(ctx, storageClass, metav1.CreateOptions{})
	if scErr != nil {
		t.Fatalf("Could not create StorageClass %s: %v", storageClassName, scErr)
	}

	fmt.Printf("Made it after creating the StorageClass %s\n", storageClassName)
	vacName1 := "test-vac1"
	vac1 := &storagev1alpha1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsName,
			Name:      vacName1,
		},
		DriverName: driverName,
		Parameters: map[string]string{
			"iops":       initialIops,
			"throughput": initialThroughput,
		},
	}
	_, vacErr := cs.VolumeAttributesClasses().Create(ctx, vac1, metav1.CreateOptions{})
	if vacErr != nil {
		t.Fatalf("Could not create VolumeAttributesClass %s: %v", vacName1, vacErr)
	}
	fmt.Printf("Made it after creating the VolumeAttributesClass %s\n", vacName1)

	pvcName := "test-pvc"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsName,
			Name:      pvcName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(initialSize),
				},
			},
			StorageClassName:          &storageClassName,
			VolumeAttributesClassName: &vacName1,
		},
	}
	newPvc, err := clientset.CoreV1().PersistentVolumeClaims(nsName).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil || newPvc == nil {
		t.Fatalf("Error when creating PVC: %v", err)
	}
	fmt.Printf("Made it after creating the PersistentVolumeClaim %s\n", pvcName)

	pvcVolumeSource := corev1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcName,
	}
	podName := "test-pod"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsName,
			Name:      podName,
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
	_, podErr := clientset.CoreV1().Pods(nsName).Create(ctx, pod, metav1.CreateOptions{})
	if podErr != nil {
		t.Fatalf("Cannot create pod %s: %v", podName, podErr)
	}
	fmt.Printf("Made it after creating the Pod %s\n", podName)

	pvName, err := getPVName(clientset, nsName, pvcName, ctx)
	fmt.Printf("The PV name is %s\n", pvName)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}

	projectName, err := getProjectName()
	fmt.Printf("The project name is %s\n", projectName)
	if err != nil {
		t.Fatalf("Error: %d", err)
	}

	zoneName, err := getZoneFromPV(projectName, pvName)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	diskInfo := DiskInfo{
		pvName:      pvName,
		projectName: projectName,
		zone:        zoneName,
	}

	credsPath := filepath.Join("..", "..", "creds", "cloud-sa.json")
	credentialsOption := option.WithCredentialsFile(credsPath)
	computeClient, err := computev1.NewDisksRESTClient(ctx, credentialsOption)
	if err != nil {
		t.Fatalf("Could not create a compute engine client: %v", err)
	}
	defer computeClient.Close()

	iops, throughput, err := getMetadataFromPV(computeClient, diskInfo, true, true, ctx)
	if strconv.FormatInt(iops, 10) != initialIops {
		t.Fatalf("Error: The provisioned IOPS does not match initial IOPS! Got: %d, want: %s", iops, initialIops)
	}
	if strconv.FormatInt(throughput, 10) != initialThroughput {
		t.Fatalf("Error: The provisioned throughput does not match initial throughput! Got: %d, want: %s", throughput, initialThroughput)
	}

	vacName2 := "test-vac2"
	updatedIops := "3013"
	updatedThroughput := "181"
	vac2 := &storagev1alpha1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: nsName,
			Name:      vacName2,
		},
		DriverName: driverName,
		Parameters: map[string]string{
			"iops":       updatedIops,
			"throughput": updatedThroughput,
		},
	}
	_, vacErr = cs.VolumeAttributesClasses().Create(ctx, vac2, metav1.CreateOptions{})
	if vacErr != nil {
		t.Fatalf("Error when creating vac %s: %v", vacName2, vacErr)
	}
	// To update, just use clientset.CoreV1().PersistentVolumes().Update()
	pvc.Spec.VolumeAttributesClassName = &vacName2
	clientset.CoreV1().PersistentVolumeClaims(nsName).Update(ctx, pvc, metav1.UpdateOptions{})
}

// TODO: Fix these to be in order of calling the functions
func getPVName(clientset *kubernetes.Clientset, nsName string, pvcName string, ctx context.Context) (string, error) {
	pvName := ""
	pvcErr := wait.PollUntilContextCancel(ctx, 30*time.Second, false, func(ctx context.Context) (bool, error) {
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
		return "", pvcErr
	}
	return pvName, nil
}

// getProjectName gets the project name through gcloud. Assumes the user is authenticated for gcloud already.
func getProjectName() (string, error) {
	cmd := exec.Command("gcloud", "config", "get-value", "project")
	resp, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("Could not execute the command 'gcloud config get-value project' due to error: %v", err)
	}
	return strings.Trim(string(resp), "\n"), nil
}

func getMetadataFromPV(computeClient *computev1.DisksClient, diskInfo DiskInfo, getIops bool, getThroughput bool, ctx context.Context) (int64, int64, error) {
	// zoneRequest = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
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

// getZoneFromPV returns the string of the persistent volume `pvName`. Assumes the user is authenticated on gcloud.
func getZoneFromPV(projectName string, pvName string) (string, error) {
	cmd := exec.Command("gcloud", "auth", "application-default", "print-access-token")
	output, err := cmd.Output()
	if err != nil {
		// TODO: instruct the gcloud command to use
		return "", fmt.Errorf("Could not get access token: %v", err)
	}
	accessToken := strings.Trim(string(output), "\n")

	listDiskReq := fmt.Sprintf(disksRequest, projectName)
	req, err := http.NewRequest("GET", listDiskReq, nil)
	if err != nil {
		return "", fmt.Errorf("Error when creating the http request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Error from client.Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("Error: %v", string(respBody))
	}

	var diskList AggregatedList
	jsonErr := json.NewDecoder(resp.Body).Decode(&diskList)
	if jsonErr != nil {
		return "", fmt.Errorf("Error when decoding: %v", jsonErr)
	}
	fullZoneName := ""
	for _, disks := range diskList.Items {
		for _, disk := range disks.Disks {
			if disk.Name == pvName {
				fmt.Printf("The zone of disk %s is: %s\n", pvName, disk.Zone)
				fullZoneName = disk.Zone
			}
		}
	}
	zoneParts := strings.Split(fullZoneName, "/")
	return zoneParts[len(zoneParts)-1], nil
}

/*
	zones, err := zoneClient.Zones.List(projectId).Context(ctx).Do()
	if err != nil {
		t.Fatalf("Error: %d", err)
	}
	var disks map[string]*compute.Disk
	for _, zone := range zones.Items {
		disksResponse, err := zoneClient.Disks.List(projectId, zone.Name).Context(ctx).Do()
		if err != nil {
			fmt.Printf("Error when loading disks from zone %s: %d\n", zone.Name, err)
		}
		for _, disk := range disksResponse.Items {
			disks[disk.Name] = disk
		}
	}
	pvName := "pvc-87b6c5f8-dcb7-48e8-8819-2b15c5e07546"
	for diskName, disk := range disks {
		if diskName == pvName {
			fmt.Printf("The zone of disk %s is: %s\n", pvName, disk.Zone)
		}
	}
*/

/*
	zoneClient, err := compute.NewZonesRESTClient(ctx, credentialsOption)
	listDiskRequest := &computepb.ListZonesRequest{
		Project: projectName,
	}
	defer zoneClient.Close()
	zoneIterator := zoneClient.List(ctx, listDiskRequest)
	for {
		zoneResp, err := zoneIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Error when trying using zone iterator: %v", err)
		}
		fmt.Printf("Zone: %s", zoneResp)
	}
*/
/*
	req := &computepb.AggregatedListDisksRequest{
		Project: projectId,
	}
	it := computeClient.AggregatedList(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Error: %v", err)
		}
		fmt.Printf("The error is: %d\n", err)
		fmt.Printf("The key is %s:\n", resp.Key)
		for _, disk := range resp.Value.Disks {
			fmt.Printf("Disk %s, ", *disk.Name)
		}
		fmt.Printf("\n")
	}
		listDiskRequest := &computepb.AggregatedListDisksRequest{
			Project: "travisx-joonix",
		}
		diskIterator := computeClient.AggregatedList(ctx, listDiskRequest)
		if diskIterator == nil {
			t.Fatalf("Disk iterator is nil")
		}
		for diskPair, err := diskIterator.Next(); err != iterator.Done; {
			fmt.Printf("The key of the disk pair is %s\n", diskPair.Key)
			if diskPair.Value == nil {
				t.Fatalf("Disk pair value is nil")
			}
			disks := diskPair.Value.Disks
			for _, disk := range disks {
				fmt.Printf("Disk Name: %s\n", *disk.Name)
				fmt.Printf("Provisioned IOPS: %d, ", disk.ProvisionedIops)
				fmt.Printf("Provisioned Throguhput: %d, ", disk.ProvisionedThroughput)
				fmt.Printf("Zone: %s\n", *disk.Zone)
			}
		}
*/
