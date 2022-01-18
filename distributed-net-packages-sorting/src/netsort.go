package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

const RECORD_SIZE int = 100
const KEY_SIZE int = 10
const INDEX_MAX_SIZE int = 8

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

// get server's id from byte date
func getId(N int, data []byte) int {
	index := 0
	res := 0

exit:
	for _, d := range data {
		for i := INDEX_MAX_SIZE - 1; i >= 0; i-- {
			if index >= N {
				break exit
			}
			index++
			temp := (int(d) & (1 << i) >> i)
			res = res<<1 + temp

		}
	}
	return res
}

// connect to servers, receive datas from servers
func listenForData(ch chan []byte, se struct {
	ServerId int    "yaml:\"serverId\""
	Host     string "yaml:\"host\""
	Port     string "yaml:\"port\""
}, curId int) {
	var conn net.Conn
	for {
		conn1, err := net.Dial("tcp", se.Host+":"+se.Port)
		conn = conn1
		if err != nil {
			time.Sleep(time.Microsecond)
		} else {
			fmt.Println("connected to ", se.Host)
			break
		}
	}
	defer conn.Close()

	handleConnection(conn, ch, se.ServerId, curId)

}

func handleConnection(conn net.Conn, ch chan []byte, id int, curID int) {
	_, err := conn.Write([]byte(strconv.Itoa(curID)))
	if err != nil {
		panic(err)
	}
	buffer := make([]byte, RECORD_SIZE)
	var ans []byte
	for {
		N, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Printf("Received data failed %v", err)
				time.Sleep(time.Millisecond)
				continue
			} else {
				fmt.Println("received all ")
				break
			}
		}

		if string(buffer[:N]) == "EOF" {
			fmt.Println("received ALL ")
			break
		}
		fmt.Println("received ", buffer, " from", id)
		ans = append(ans, buffer...)
	}
	ch <- ans
}

// send data to other servers
func sendRecord(conn net.Conn, sc chan bool, recordOfId map[int][][]byte) {
	buffer := make([]byte, 16)
	N, err := conn.Read(buffer)
	if err != nil {
		log.Printf("receive fails")
	}
	defer conn.Close()

	rId, _ := strconv.Atoi(string(buffer[:N]))

	fmt.Println("accept a connection from client ", rId, "ready to send ", len(recordOfId[rId]), " records")
	doSend(recordOfId, conn, rId, sc)

}

func doSend(recordOfId map[int][][]byte, conn net.Conn, rId int, sc chan bool) {
	for _, r := range recordOfId[rId] {
		load := r
		_, err := conn.Write(load)
		if err != nil {
			log.Printf("failed to send")
		}
		fmt.Println("send to", rId, "debug content", load)
	}
	fmt.Println("succeed")
	conn.Write([]byte("EOF"))
	sc <- true
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/
	// get bits for ids
	numOfServers := len(scs.Servers)
	idBits := math.Log2(float64(numOfServers))

	// get and read file
	inputFile := os.Args[2]
	outputFile := os.Args[3]
	inputData, err := ioutil.ReadFile(inputFile)
	if err != nil {
		panic(err)
	}
	// map to records
	recordOfId := make(map[int][][]byte)
	for i := 0; i < len(inputData)/RECORD_SIZE; i++ {
		rId := getId(int(idBits), inputData[RECORD_SIZE*i:RECORD_SIZE*i+KEY_SIZE])
		recordOfId[rId] = append(recordOfId[rId], inputData[RECORD_SIZE*i:RECORD_SIZE*(i+1)])

		// test
		fmt.Println("server id: %d ", rId)
	}
	fmt.Println(recordOfId, "record map")

	// as a client, read and receive data from server
	var host, port string

	for i := range scs.Servers {
		if scs.Servers[i].ServerId == serverId {
			host, port = scs.Servers[i].Host, scs.Servers[i].Port
			break
		}
	}

	l, err := net.Listen("tcp", host+":"+port)

	if err != nil {
		log.Printf("failing in building")
		os.Exit(1)
	}

	ch1 := make(chan []byte)

	for _, s := range scs.Servers {
		if s.ServerId == serverId {
			continue
		}

		go listenForData(ch1, s, serverId)

	}

	// as a server, send data to client

	numCompleted := 0
	ch2 := make(chan bool)
	for {
		if numCompleted == numOfServers-1 {
			break
		}
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("failed to connect")
			time.Sleep(time.Millisecond)
		}
		numCompleted++

		go sendRecord(conn, ch2, recordOfId)

	}

	// save to local
	var localData [][]byte
	clientDone := 0
	for {
		if clientDone == numOfServers-1 {
			break
		}
		record := <-ch1
		clientDone++
		for i := 0; i < len(record)/RECORD_SIZE; i++ {
			localData = append(localData, record[i*RECORD_SIZE:(i+1)*RECORD_SIZE])
			fmt.Println("appended ", record[i*100:(i+1)*100])
		}
	}

	for i := 0; i < len(recordOfId[serverId]); i++ {
		localData = append(localData, recordOfId[serverId][i])
	}

	for i := 0; i < numOfServers-1; i++ {
		<-ch2
	}

	fmt.Println("output before sort", localData)
	/// do the sort
	sort.Slice(localData, func(i, j int) bool {
		return bytes.Compare(localData[i][:KEY_SIZE], localData[j][:KEY_SIZE]) < 0
	})

	fmt.Println("output after sort", localData)
	ioutil.WriteFile(outputFile, bytes.Join(localData, []byte("")), 0666)

}
