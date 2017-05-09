package kafka

import "strings"

type errorList []error

func (errors errorList) Error() string {
	switch len(errors) {
	case 0:
		return ""
	case 1:
		return errors[0].Error()
	default:
		s := make([]string, len(errors))
		for i, e := range errors {
			s[i] = e.Error()
		}
		return strings.Join(s, ": ")
	}
}

func appendError(to error, err error) error {
	if err == nil {
		return to
	}

	if to == nil {
		return err
	}

	if errlist, ok := to.(errorList); ok {
		return append(errlist, err)
	}

	return errorList{to, err}
}
