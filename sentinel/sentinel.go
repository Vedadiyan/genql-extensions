package sentinel

type (
	Error string
)

const (
	TOO_MANY_ARGS Error = Error("too many arguments")
	TOO_FEW_ARGS  Error = Error("too few arguments")
)

func (err Error) Error() string {
	return string(err)
}
