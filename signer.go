package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// сюда писать код

func SingleHash(in, out chan interface{}) {
	//fmt.Println("Start SingleHash")
	wt := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for val := range in {
		wt.Add(1)
		go func(val interface{}, in, out chan interface{}, wt *sync.WaitGroup, mu *sync.Mutex) {
			defer wt.Done()
			data := strconv.Itoa(val.(int))
			mu.Lock()
			valMd5 := DataSignerMd5(data)
			mu.Unlock()

			mp := make(map[int]string)
			wg2 := &sync.WaitGroup{}
			mu2 := &sync.Mutex{}
			wg2.Add(1)
			go func(data string, idx int, mp *map[int]string, mu2 *sync.Mutex, wg2 *sync.WaitGroup) {
				defer wg2.Done()
				val := DataSignerCrc32(data)
				mu.Lock()
				(*mp)[idx] = val
				mu.Unlock()
			}(data, 0, &mp, mu2, wg2)
			wg2.Add(1)
			go func(data string, idx int, mp *map[int]string, mu2 *sync.Mutex, wg2 *sync.WaitGroup) {
				defer wg2.Done()
				val := DataSignerCrc32(data)
				mu.Lock()
				(*mp)[idx] = val
				mu.Unlock()
			}(valMd5, 1, &mp, mu2, wg2)
			wg2.Wait()
			result := mp[0] + "~" + mp[1]

			//fmt.Println("End SingleHash: ", result)
			out <- result
		}(val, in, out, wt, mu)
	}
	wt.Wait()
}

func MultiHash(in, out chan interface{}) {
	//fmt.Println("Start MultiHash")
	wt := &sync.WaitGroup{}
	for val := range in {
		wt.Add(1)
		go func(val interface{}, in, out chan interface{}, wt *sync.WaitGroup) {
			defer wt.Done()
			data := val.(string)
			mp := make(map[int]string)
			wt2 := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			for i := 0; i < 6; i++ {
				wt2.Add(1)
				go func(idx int, data string, mp *map[int]string, wt2 *sync.WaitGroup, mu *sync.Mutex) {
					defer wt2.Done()
					val := DataSignerCrc32(strconv.Itoa(idx) + data)
					mu.Lock()
					(*mp)[idx] = val
					mu.Unlock()
				}(i, data, &mp, wt2, mu)
			}
			wt2.Wait()
			var result string
			for i := 0; i < 6; i++ {
				result += mp[i]
			}
			//fmt.Println("End MultiHash: ", result)
			out <- result
		}(val, in, out, wt)
	}
	wt.Wait()
}

func CombineResults(in, out chan interface{}) {
	//fmt.Println("Start CombineResults")
	var result []string
	for str := range in {
		result = append(result, str.(string))
	}
	sort.Strings(result)
	var res string
	flag := false
	for _, val := range result {
		if !flag {
			flag = true
			res += val
			continue
		}
		res += "_" + val
	}
	//fmt.Println("End CombineResults: ", res)
	out <- res
}

func Print(str string) {
	fmt.Println(str)
}

func StartWorker(in, out chan interface{}, work job, wg *sync.WaitGroup) {
	defer wg.Done()
	work(in, out)
	close(out)
}

func ExecutePipeline(workers ...job) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}

	for _, work := range workers {
		out := make(chan interface{})
		wg.Add(1)
		go StartWorker(in, out, work, wg)
		in = out
	}
	//fmt.Println("Waiting")
	wg.Wait()
}
