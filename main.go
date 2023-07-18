package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type execution struct {
	client      mqtt.Client
	resulttopic string
	executionid string
}

func (e *execution) log(msg interface{}) {
	e.client.Publish(e.resulttopic+"/log", 0, false, msg)
}

func (e *execution) error(msg interface{}) {
	e.client.Publish(e.resulttopic+"/error", 0, false, msg)
}

func (e *execution) status(status string) {
	e.client.Publish(e.resulttopic, 0, false, status)
}

type logwriter struct {
	*execution
}

func (w *logwriter) Write(data []byte) (int, error) {
	w.log(data)
	return len(data), nil
}

type errwriter struct {
	*execution
}

func (w *errwriter) Write(data []byte) (int, error) {
	w.error(data)
	return len(data), nil
}

func (e *execution) runfile(filename string) {
	cmd := exec.Cmd{
		Path: "/bin/sh",
		Args: []string{filename},
		Stdout: &logwriter{
			execution: e,
		},
		Stderr: &errwriter{
			execution: e,
		},
	}

	err := cmd.Start()
	if err != nil {
		e.error(err.Error())
		e.status("Failed")
		return
	}

	err = cmd.Wait()
	if err != nil {
		e.error(err.Error())
		e.status("Failed")
		return
	}

}

func (e *execution) getTempFile(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		e.error(err.Error())
		e.status("Failed")
		return ""
	}
	defer resp.Body.Close()
	out, err := os.CreateTemp("mqworker", e.executionid+"-*.sh")
	if err != nil {
		e.error(err.Error())
		e.status("Failed to create temp file")
		return ""
	}
	defer out.Close()
	io.Copy(out, resp.Body)
	return out.Name()
}

func run(c mqtt.Client, identifier string, cmd string) {
	executionid := time.Now().UTC().Format("20060102150405")
	execution := &execution{
		client:      c,
		resulttopic: "mqworker/" + identifier + "/run/" + executionid,
		executionid: executionid,
	}

	execution.status("Starting")
	u, err := url.Parse(cmd)
	if err != nil {
		execution.error(err.Error())
		execution.status("Failed")
		return
	}

	switch u.Scheme {
	case "file":
		execution.runfile(u.Path)
	case "http":
		file := execution.getTempFile(cmd)
		if file != "" {
			execution.runfile(file)
		}
	case "https":
		file := execution.getTempFile(cmd)
		if file != "" {
			execution.runfile(file)
		}

	default:
		execution.error("Uknown command")
		execution.status("Failed")
		return
	}
}

func main() {
	opts := mqtt.NewClientOptions()

	defaulthn, _ := os.Hostname()
	identifier := flag.String("i", defaulthn, "executor identifier. Hostname is used by default")

	flag.Parse()
	urls := flag.Args()

	for _, url := range urls {
		opts = opts.AddBroker(url)
	}

	opts = opts.SetWill("mqworker/"+*identifier+"/available", "false", 0, false)

	opts = opts.SetOnConnectHandler(func(c mqtt.Client) {
		c.Publish("mqworker/"+*identifier+"/available", 0, false, "true").Wait()
		c.Subscribe("mqworker/"+*identifier+"/run/start", 0, func(c mqtt.Client, m mqtt.Message) {
			run(c, string(m.Payload()))
		}).Wait()
	})

	client := mqtt.NewClient(opts)

	log.Println("Attempting to connect to MQTT broker")
	for !client.IsConnected() {
		token := client.Connect()
		token.Wait()
		err := token.Error()
		if err != nil {
			log.Println("Connection failed: ", token.Error())
		}
	}
}
