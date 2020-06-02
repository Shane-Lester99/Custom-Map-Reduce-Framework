package mapreduce

import (
	"container/list"
	"fmt"
	"regexp"
	"strings"
)

// Chooses what set of map reduce functions we want
func ChooseFunctions(value string) {
	switch value {
	case "wc":
		MyMapper = myMapper
		MyReducer = myReducer
	case "test":
		MyMapper = myMapperTest
		MyReducer = myReducerTest
	default:
		err := fmt.Sprintf("There is no mapper/ reducer combination named %s", value)
		panic(err)
	}
}

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func myMapper(value string) *list.List {
	value = strings.ReplaceAll(value, ".", " ")
	alphanumeric := regexp.MustCompile("[^a-zA-Z\\s]+")
	value = alphanumeric.ReplaceAllString(value, "")
	whitespace := regexp.MustCompile("[\\s]+")
	value = whitespace.ReplaceAllString(value, " ")
	itrValue := strings.Fields(value)
	newList := new(list.List)
	newList.Init()
	for _, word := range itrValue {
		mr := KeyValue{Key: word, Value: "1"}
		newList.PushFront(mr)
	}
	return newList
}

// iterate over list and add values
func myReducer(key string, values *list.List) string {
	count := 0
	for e := values.Front(); e != nil; e = e.Next() {
		count += 1
	}
	return fmt.Sprintf("%d", count)
}

// Split in words
func myMapperTest(value string) *list.List {
	DPrintf("Map %v\n", value)
	res := list.New()
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res.PushBack(kv)
	}
	return res
}

// Just return key
func myReducerTest(key string, values *list.List) string {
	for e := values.Front(); e != nil; e = e.Next() {
		DPrintf("Reduce %s %v\n", key, e.Value)
	}
	return ""
}
