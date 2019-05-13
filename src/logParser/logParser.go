package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"os"
	"log"
	"io"
	"flag"
)

type result struct {
	response_code map[string]int64
	total_bytes   int64
	total_time    float64
	total_calls   int64
}

func Parser(c chan string, stop_read chan bool, epoch int64, wg *sync.WaitGroup, res result) result {
	defer wg.Done()
	total_calls := 0
	dropped_calls := 0
	for true {
		var access_log string
		const log_time_format = "[02/Jan/2006:15:04:05"
		access_log,ok :=<- c
		if !ok {
			fmt.Printf("returing channel closed\n")
			fmt.Printf("parser fexiting totalcalls parsed %d dropped_calls %d\n",total_calls,dropped_calls)
			return res
		}
		access_log_arr := strings.Split(access_log, " ")
		if len(access_log_arr) < 11{
			fmt.Printf("log is less than 11 fields:%s\n",access_log)
			continue
		}
		call_time, err := time.Parse(log_time_format, access_log_arr[3])
		if err != nil {
			fmt.Printf("%s", err)
			continue
		}
		if call_time.Unix() < (epoch - 600) {
			//fmt.Printf("sending stop read\n")
			dropped_calls += 1
			//continue
			stop_read <- true
			//continue
			fmt.Printf("parser fexiting totalcalls parsed %d\n",total_calls)
			return res
		}
		total_calls += 1
		if _, ok := res.response_code[access_log_arr[8]]; ok {
			res.response_code[access_log_arr[8]] += 1
		} else {
			res.response_code[access_log_arr[8]] = 1
		}
		bytes,_ := strconv.Atoi(access_log_arr[9])
		res.total_bytes += int64(bytes)
		exec_time,_ := strconv.ParseFloat(access_log_arr[10], 64)
		res.total_time += exec_time
		res.total_calls += 1
	}
	return res
}

func send_metrics(res result, metrics_meta, graphite_endpoint string, epoch int64) {
	///Sends metrics to the colos metrics receiver
	metrics_receiver := graphite_endpoint
	conn, err := net.Dial("tcp", metrics_receiver)
	if err != nil {
		log.Fatal("connection to graphite failed\n")
	}
	host, _ := os.Hostname()
	hostname := strings.Split(host, ".")
	host = hostname[0]
	fmt.Fprintf(conn, "%s.TotalCalls.%s %d %d\n", metrics_meta, host, res.total_calls, epoch)
	fmt.Fprintf(conn, "%s.TomcatExecutionTime.%s %f %d \n", metrics_meta, host, ((res.total_time)/float64(res.total_calls))*1000, epoch)
	fmt.Fprintf(conn, "%s.AvgBytes.%s %f %d \n", metrics_meta, host, float64(res.total_bytes)/float64(res.total_calls), epoch)
	for key, _ := range res.response_code {
		fmt.Fprintf(conn, "%s.ResponseCode.%s.%s  %d %d \n", metrics_meta, host, key, (res.response_code[key]), epoch)
	}
	fmt.Fprintf(os.Stdout, "%s.TotalCalls.%s %d %d\n", metrics_meta, host, res.total_calls, epoch)
	fmt.Fprintf(os.Stdout, "%s.TomcatExecutionTime.%s %f %d \n", metrics_meta, host, ((res.total_time)/float64(res.total_calls))*1000, epoch)
	fmt.Fprintf(os.Stdout, "%s.AvgBytes.%s %f %d \n", metrics_meta, host, float64(res.total_bytes)/float64(res.total_calls), epoch)
	//fmt.Fprintf(os.Stdout, "%s.AvgNetworkTime.%s %f %d \n", metrics_meta, host, (res.NetworkTime)/float64(res.TotalCalls), epoch)
	for key, _ := range res.response_code {
		fmt.Fprintf(os.Stdout, "%s.ResponseCode.%s.%s  %d %d \n", metrics_meta, host, key, (res.response_code[key]), epoch)
	}
}

func read_from_file(c chan string, file io.ReadSeeker, wg *sync.WaitGroup, size int64, stop_read chan bool) {
	///Reads the file backwards in chunks of 8KB until the start of file or a stop read is sent via the channel
	total_calls,drop_count := 0,0
	defer close(c)
	defer wg.Done()
	fmt.Printf("The size of the file is %d\n", size)
	buffer := make([]byte, 8192)
	var cur_location int64
	cur_location = size
	end_of_file := false
	for true {
		select {
		case test := <-stop_read:
			fmt.Printf("signal to stop Read %t\n", test)
			return

		default:
			if cur_location > 0 && !end_of_file {
				cur_location = cur_location - 8192
				if cur_location == 0{
					end_of_file = true
				}
				file.Seek(cur_location, 0)
				number_read, err := file.Read(buffer)
				if err != nil {
					fmt.Printf("%s\n", err)
					continue
				}
				if number_read < 8192 {
					fmt.Printf("read %d\n", number_read)
				}
				buf_string := string(buffer)
				log_lines := strings.Split(buf_string, "\n")
				for i := len(log_lines) - 2; i > 0; i-- {
					total_calls += 1
					select{
					case c <- log_lines[i]:
						continue
					case test := <-stop_read:
						fmt.Printf("signal to stop Read %t\n", test)
						fmt.Printf("Residual bytes %d\n", cur_location)
						fmt.Printf("File Read Stopping total_calls %d\n", total_calls)
						fmt.Printf("tried to drop %d times\n", drop_count)
						return
					}
				}


				nce := int64(len([]byte(log_lines[0]))) + int64(len([]byte("\n")))
				cur_location = cur_location + nce
				if cur_location < 8192 && cur_location != nce {
					cur_location = 8192
				}
			}else {
				fmt.Printf("Residual bytes %d\n", cur_location)
				fmt.Printf("File Read Stopping total_calls %d\n", total_calls)
				fmt.Printf("tried to drop %d times\n", drop_count)
				return
			}
		}
	}
}
func get_times(t time.Time)  (string, string) {
	//stupid hack to get file names in the format
	this_hour := t.Format("15")
	last_hour,_ := strconv.Atoi(this_hour)
	last_hour = last_hour-1
	if last_hour <0 {
		last_hour = 23
	}
	if (last_hour < 10){
		last_hour_string:= fmt.Sprintf("%s%s","0",strconv.Itoa(last_hour))
		return this_hour,last_hour_string
	}
	return this_hour, strconv.Itoa(last_hour)
}
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		fmt.Printf("timed out\n")
		return true // timed out
	}
}



func main() {
	stop_read := make( chan bool,1)
	metrics := make(chan string, 100000)
	metrics2 := make(chan string, 100000)
	t := time.Now()
	epoch_time := t.Unix()
	//epoch_time = 1493039720
	res := result{make(map[string]int64),0,0.0,0}
	fmt.Println(epoch_time)
	current_hour_file_name := flag.String("ch","/opt/tomcat/logs/localhost_access_log.txt","current hour log file")
	previous_hour_file_name := flag.String("ph","/opt/tomcat/logs/localhost_access_log.txt.1","previous hour log file")
	graphite_prefix := flag.String("prefix","None","prefix for graphte metrics")
	graphite_endpoint  := flag.String("endPoint","localhost:2003", "graphite endPoint")
	flag.Parse()
	if *graphite_prefix == "None" {
		flag.PrintDefaults()
		log.Fatal("Invalid Parameters")
	}
	//fmt.Printf(*current_hour_file_name)
	file, err := os.Open(*current_hour_file_name)
	if err != nil {
		log.Fatal(err)
	}
	file_stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := file_stat.Size()
	var wg sync.WaitGroup
	wg.Add(1)
	go read_from_file(metrics, file, &wg, size, stop_read)
	wg.Add(1)
	go func() {
		res = Parser(metrics, stop_read, epoch_time, &wg, res)
	}()
	waitTimeout(&wg,2*time.Minute)

	if t.Minute() < 10 {
		//fmt.Printf("opening file %s\n",*previous_hour_file_name)
		file2, err := os.Open(*previous_hour_file_name)
		if err != nil {
			log.Fatal(err)
		}
		file2_stat, err := file2.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size2 := file2_stat.Size()
		wg.Add(1)
		go read_from_file(metrics2, file2, &wg, size2, stop_read)
		wg.Add(1)
		go func () {
			res = Parser(metrics2, stop_read, epoch_time, &wg, res)
		}()
	}
		waitTimeout(&wg,3*time.Minute)

	send_metrics(res,*graphite_prefix, *graphite_endpoint ,epoch_time)
	fmt.Printf("Done!!!\n",)

}
