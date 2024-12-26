package utils

import (
	"fmt"
	"runtime"
)

func PrintMem() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// 打印内存相关统计信息
	fmt.Println("Memory Statistics:")
	fmt.Printf("\tAlloc:          %v\n", BytesToHuman(memStats.Alloc))
	fmt.Printf("\tTotalAlloc:     %v\n", BytesToHuman(memStats.TotalAlloc))
	fmt.Printf("\tSys:            %v\n", BytesToHuman(memStats.Sys))
	fmt.Printf("\tHeapAlloc:      %v\n", BytesToHuman(memStats.HeapAlloc))
	fmt.Printf("\tHeapSys:        %v\n", BytesToHuman(memStats.HeapSys))
	fmt.Printf("\tStackInUse:     %v\n", BytesToHuman(memStats.StackInuse))
	fmt.Printf("\tHeapInUse:      %v\n", BytesToHuman(memStats.HeapInuse))
	fmt.Printf("\tNumGC:          %v\n", memStats.NumGC)
}

func GetMem() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Sys
}

func GetMemString() string {
	return BytesToHuman(GetMem())
}
