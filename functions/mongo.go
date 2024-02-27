package functions

import (
	"fmt"
	"strings"

	"github.com/vedadiyan/genql"
)

func ExecFunc(args []any) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no argument")
	}
	payload, ok := args[0].([]map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected key value pair array but found %T", payload)
	}
	mapper := make(map[string]any)
	for _, item := range payload {
		mapper[strings.ToLower(item["_title"].(string))] = item["_value"].(string)
	}
	formula := strings.ToLower(mapper["formula"].(string))
	if len(formula) == 0 {
		return nil, fmt.Errorf("formula not found")
	}
	delete(mapper, "formula")
	query, err := genql.New(mapper, fmt.Sprintf("SELECT %s AS result", formula), genql.PostgresEscapingDialect())
	if err != nil {
		return nil, err
	}
	rs, err := query.Exec()
	if err != nil {
		return nil, err
	}
	return rs[0], nil
}
