package main

import (
	"bytes"
	"encoding/json"
	"log"
	"os/exec"
	"sync"
	"text/template"
	"time"
)

//{
//    "data": [
//        {
//            "starttime": 3,
//            "GPU": 8,
//            "lasttime": 5
//        },
//        {
//            "starttime": 3,
//            "GPU": 8,
//            "lasttime": 5
//        }
//    ]
//}

const (
	imageName = "registry.sensetime.com/cloudnative4ai/nvidia/cuda-vector-add"
)

type TraceEntry struct {
	Data []Data `json:"data"`
}
type Data struct {
	Index       int
	ImageName	string
	StartTime   int `json:"startTime"`
	GpuCnt      int `json:"gpuCnt"`
	RunningTime int `json:"runningTime"`
}

var jobYamlTmpl = `apiVersion: v1
kind: Pod
metadata:
  name: job-dispatcher-test-{{.Index}}
spec:
  restartPolicy: OnFailure
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

func dispatchJob(entry Data) (string, error) {
	buf := new(bytes.Buffer)
	tmpl, err := template.New("jobYaml").Parse(jobYamlTmpl)
	if err != nil {
		return "", err
	}
	err = tmpl.Execute(buf, entry)
	if err != nil {
		return "", err
	}

	cmd := exec.Command("kubectl", "create", "-f", "-")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}

	_, _ = stdin.Write(buf.Bytes())
	_ = stdin.Close()

	out, err := cmd.CombinedOutput();
	if err != nil {
		return "", err
	}
	return string(out), nil
}

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

func main() {
	log.Println("start")
	var wg sync.WaitGroup

	inputString := `{
	  "data": [
		{
		  "startTime": 3,
		  "gpuCnt": 8,
		  "runningTime": 5
		},
		{
		  "startTime": 8,
		  "gpuCnt": 2,
		  "runningTime": 7
		}
	  ]
	}`
	inputData := []byte(inputString)
	var traceEntries TraceEntry
	err := json.Unmarshal(inputData, &traceEntries)
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range traceEntries.Data {
		log.Printf("load job %d: startTime %d, gpuCnt %d, runningTime %d\n",
			i, v.StartTime, v.GpuCnt, v.RunningTime)
		v.Index = i
		v.ImageName = imageName
		wg.Add(1)
		go singleDispatcher(&wg, v)
	}
	wg.Wait()
	log.Println("end")
}