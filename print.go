package main

import "fmt"

const color = "\033[33m"
const colorReset = "\033[0m"

func PrintFile(msgPrefix, msg string) {
	fmt.Printf("%s %s %s %s\n", color, msgPrefix, colorReset, msg)
}
