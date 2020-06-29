package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os/exec"
	"sync"
	"text/template"
	"time"
)

const (
	imageName = "registry.sensetime.com/cloudnative4ai/nvidia/cuda-vector-add"
	schedulerName = "sense-rubber"
)

type TraceEntry struct {
	Data []Data `json:"data"`
}
type Data struct {
	Index       	int
	ImageName		string
	SchedulerName	string
	StartTime   	int `json:"startTime"`
	GpuCnt      	int `json:"gpuCnt"`
	RunningTime 	int `json:"runningTime"`
}

var jobYamlTmpl = `apiVersion: v1
kind: Pod
metadata:
  name: job-dispatcher-test-{{.Index}}
spec:
  restartPolicy: OnFailure
  schedulerName: {{.SchedulerName}}
  containers:
  - name: cuda-vector-add
    image: {{.ImageName}}
    imagePullPolicy: Always
    resources:
      limits:
        nvidia.com/gpu: {{.GpuCnt}}
    command: ["sleep"]
    args: ["{{.RunningTime}}s"]
`

// trace
var traceEntries TraceEntry
// filepath
var filePath string


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
	log.Printf("dispatch job %d at %d\n", entry.Index, entry.StartTime)
	out, err := dispatchJob(entry)
	if err != nil {
		log.Printf("dispatch job %d failed: %s; %s\n", entry.Index, err, out)
	} else {
		log.Printf("dispatch job %d success: %s", entry.Index, out)
	}
}

func buildArgs() string {
	flag.StringVar(&filePath,"trace", "traces.json", "`path` to trace file")
	flag.Parse()

	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}

func parseTrace(inputString string) {
	inputData := []byte(inputString)
	err := json.Unmarshal(inputData, &traceEntries)
	if err != nil {
		log.Fatal(err)
	}
}

// wait until all worker dispatch
func launchJob() {
	var wg sync.WaitGroup
	for i, v := range traceEntries.Data {
		log.Printf("load job %d: startTime %d, gpuCnt %d, runningTime %d\n",
			i, v.StartTime, v.GpuCnt, v.RunningTime)
		v.Index = i
		v.ImageName = imageName
		v.SchedulerName = schedulerName
		wg.Add(1)
		go singleDispatcher(&wg, v)
	}
	wg.Wait()
}

func main() {
	log.Println("start")
	s := buildArgs()
	parseTrace(s)
	launchJob()
	log.Println("end")
}