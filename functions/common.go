package functions

import (
	"fmt"
	"reflect"
	"strconv"
)

func ArgStructAnalyzer[T any]() (int, map[string]int, error) {
	argType := reflect.TypeOf(*new(T))
	optionalFields := make(map[string]int)
	fields := 0
	for i := 0; i < argType.NumField(); i++ {
		field := argType.Field(i)
		tags := field.Tag
		if tags.Get("optional") != "true" {
			fields += 1
			continue
		}
		pos := tags.Get("position")
		if len(pos) == 0 {
			return 0, nil, fmt.Errorf("position is required for optional fields")
		}
		posInt, err := strconv.Atoi(pos)
		if err != nil {
			return 0, nil, err
		}
		optionalFields[field.Name] = posInt
	}
	return fields, optionalFields, nil
}
