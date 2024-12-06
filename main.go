package main

import "fmt"

func main() {
	ms := NewMicroservice()
	servers := "localhost:9092"
	err := ms.Consume(servers, "login", "example-group", func(ctx IContext) error {
		ctx.Log(fmt.Sprintf("Processing message: %s", ctx.ReadInput()))
		return nil
	})
	if err != nil {
		fmt.Println("Error:", err)
	}

	ms.Start()
}
