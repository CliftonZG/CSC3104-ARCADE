package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
)

func main() {
	target := flag.String("target", "", "target process or container to terminate")
	mode := flag.String("mode", "sigkill", "fault mode: sigkill, delay, or partition")
	flag.Parse()

	if *target == "" {
		fmt.Println("Usage: chaos -target <process_name_or_pid> -mode <sigkill|delay|partition>")
		os.Exit(1)
	}

	log.Printf("[CHAOS] Executing fault injection: target=%s mode=%s", *target, *mode)

	switch *mode {
	case "sigkill":
		cmd := exec.Command("pkill", "-9", "-f", *target)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("[CHAOS] pkill completed with: %v (out: %s)", err, string(out))
		} else {
			log.Printf("[CHAOS] Successfully terminated target %s", *target)
		}
	default:
		log.Printf("[CHAOS] Unsupported mode %s", *mode)
	}
}
