package main

import "fmt"

func main() {

	messages := make(chan string)

	go func() { messages <- "ping0" }()
	go func() { messages <- "ping1" }()

	msg := <-messages
	msg1 := <-messages
	fmt.Println(msg, msg1)
}
