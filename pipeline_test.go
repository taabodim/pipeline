package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

func TestProcessPipeline(t *testing.T) {

	// we create a simple struct with an int, just for demonstration purposes
	type pContext struct {
		callNum int
	}

	// we create a pipeline where each module increments the `callNum` counter
	p := New(runtime.NumCPU()).withModule(func(ctx *pContext) *pContext {
		ctx.callNum++
		return ctx
	}).withModule(func(ctx *pContext) *pContext {
		ctx.callNum++
		return ctx
	}).withModule(func(ctx *pContext) *pContext {
		ctx.callNum++
		return ctx
	}).withModule(func(ctx *pContext) *pContext {
		ctx.callNum++
		return ctx
	})

	// we start the pipeline
	p.Start()

	for i := 0; i < 10; i++ {
		// we create a new context and publish it in our pipeline every time
		ctx := new(pContext)
		ctx.callNum = i
		p.Publish(p, ctx)
	}

	sum := new(uint64)
	var allPipelineResults [10]int

	// we process all the context from the modules
	p.Process(p, func(ctx *pContext) {
		allPipelineResults[*sum] = ctx.callNum
		atomic.AddUint64(sum, 1)
	}, 20, context.Background())

	fmt.Printf("processed %d  pipelines \n", *sum)
	fmt.Printf("processed %v  pipelines \n", allPipelineResults)
	fmt.Println("end of tests")

	if *sum != 10 {
		t.Fail()
	}
	if allPipelineResults != [10]int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13} {
		t.Fail()
	}
}
