package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/johnllao/redislock/redismutex"
)

var (
	mode string
)

func init() {
	flag.StringVar(&mode, "mode", "", "mode of locking")
	flag.Parse()
}

func main() {

	var err error
	var m *redismutex.RedisMutex

	m, err = redismutex.NewRedisMutex(":6379", "FLEXTRADE")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer m.UnLock()

	if mode == "Read" {
		err = m.RLock()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("acquired lock")
	} else {
		err = m.Lock()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("acquired lock")
	}

	fmt.Println("Press ENTER to exit")
	var stdin = bufio.NewReader(os.Stdin)
	_, _, _ = stdin.ReadLine()
}


