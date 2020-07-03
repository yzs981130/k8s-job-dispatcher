package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"text/template"
	"time"
)

const (
	imageName = "registry.sensetime.com/cloudnative4ai/nvidia/cuda-vector-add"
	schedulerName = "sense-rubber"
	maxGpuCnt = 8
)

type TraceEntry struct {
	Data []Data `json:"data"`
}
type Data struct {
	Index       	int
	ImageName		string
	SchedulerName	string
	PartitionName	string
	PodCnt			int
	StartTime   	int `json:"startTime"`
	GpuCnt      	int `json:"gpuCnt"`
	RunningTime 	int `json:"runningTime"`
}

var jobYamlTmpl = `apiVersion: batch/v1
kind: Job
metadata:
  name: job-dispatcher-test-{{.PartitionName}}-{{.Index}}
spec:
  backoffLimit: 1
  completions: {{.PodCnt}}
  parallelism: {{.PodCnt}}
  ttlSecondsAfterFinished: 10
  template:
    metadata:
      annotations:
        scheduling.k8s.io/group-name: job-dispatcher-test-{{.PartitionName}}-{{.Index}}
    spec:
      containers:
      - image: {{.ImageName}}
        imagePullPolicy: IfNotPresent
        name: cuda-vector-add
        command: ["/bin/sh","-c"]
        args: ["sleep {{.RunningTime}}"]
        resources:
          limits:
            nvidia.com/gpu: {{.GpuCnt}}
      restartPolicy: Never
      schedulerName: {{.SchedulerName}}
---
apiVersion: scheduling.incubator.k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: job-dispatcher-test-{{.PartitionName}}-{{.Index}}
spec:
  minMember: {{.PodCnt}}
  #priorityClassName: master-pri
  #block: true
  #preemptible: true
  #topologyGPU: true
`

// trace
var traceEntries TraceEntry
// trace filepath
var filePath string
// delete script
var deleteScriptHandler *os.File
// use json filename as partition name
var partitionName string
// use single GPU pod mode
var enableSingleGPU bool

// generate k8s yaml & call kubectl to create
func dispatchJob(entry Data) (string, error) {
	// generate k8s yaml, store in buf
	buf := new(bytes.Buffer)
	tmpl, err := template.New("jobYaml").Parse(jobYamlTmpl)
	if err != nil {
		return "", err
	}
	err = tmpl.Execute(buf, entry)
	if err != nil {
		return "", err
	}

	// call kubectl, using pipe to pass yaml
	// build args
	cmd := exec.Command("kubectl", "create", "-f", "-")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}
	_, _ = stdin.Write(buf.Bytes())
	_ = stdin.Close()

	// exec kubectl
	out, err := cmd.CombinedOutput();
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// goroutine worker: trigger when startTime ticks
func singleDispatcher(wg *sync.WaitGroup, entry Data) {
	defer wg.Done()
	time.Sleep(time.Duration(entry.StartTime) * time.Second)
	log.Printf("dispatch job job-dispatcher-test-%s-%d [%d pods * %d GPU] at %d\n",
		entry.PartitionName, entry.Index, entry.PodCnt, entry.GpuCnt, entry.StartTime)
	out, err := dispatchJob(entry)
	if err != nil {
		log.Printf("dispatch job %d failed: %s; %s\n", entry.Index, err, out)
	} else {
		log.Printf("dispatch job %d success: %s", entry.Index, out)
	}
}

func initFunc() string {
	flag.StringVar(&filePath,"trace", "traces.json", "`path` to trace file")
	flag.BoolVar(&enableSingleGPU, "single",
		false, "set `true` to force use per pod single GPU mode")
	flag.Parse()
	log.Printf("using trace %s with enableSingleGPU %v\n", path.Base(filePath), enableSingleGPU)

	var err error
	deleteScriptHandler, err = os.OpenFile("delete.sh", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Fatal(err)
	}

	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}
	partitionName = strings.TrimSuffix(path.Base(filePath), path.Ext(filePath))
	return string(b)
}

func parseTrace(inputString string) {
	inputData := []byte(inputString)
	err := json.Unmarshal(inputData, &traceEntries)
	if err != nil {
		log.Fatal(err)
	}
}

// print podgroup delete script to the end of delete.sh
func printDeleteScript() {
	deleteCmd := "kubectl delete podgroup $(kubectl get podgroup | grep job-dispatcher-test |awk '{print$1}') --grace-period=0 --force"
	_, _ = deleteScriptHandler.WriteString(deleteCmd)
	_ = deleteScriptHandler.Sync()
}

// wait until all worker dispatch
func launchJob() {
	defer deleteScriptHandler.Close()
	var wg sync.WaitGroup
	for i, v := range traceEntries.Data {
		log.Printf("load job %d: startTime %d, gpuCnt %d, runningTime %d\n",
			i, v.StartTime, v.GpuCnt, v.RunningTime)

		deleteCmd := fmt.Sprintf("kubectl delete job job-dispatcher-test-%d\n", i)
		_, _ = deleteScriptHandler.WriteString(deleteCmd)
		_ = deleteScriptHandler.Sync()

		v.Index = i
		v.ImageName = imageName
		v.SchedulerName = schedulerName
		if enableSingleGPU {
			v.PodCnt = v.GpuCnt
			v.GpuCnt = 1
		} else {
			v.PodCnt = int(math.Ceil(float64(v.GpuCnt) / maxGpuCnt))
			if v.GpuCnt > maxGpuCnt {
				v.GpuCnt = maxGpuCnt
			}
		}
		v.PartitionName = partitionName
		wg.Add(1)
		go singleDispatcher(&wg, v)
	}
	printDeleteScript()
	wg.Wait()
}

func main() {
	log.Println("start")
	s := initFunc()
	parseTrace(s)
	launchJob()
	log.Println("end")
}