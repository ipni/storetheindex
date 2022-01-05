package v0

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type ErrorMessage struct {
	Message string `json:",omitempty"`
	Status  int    `json:",omitempty"`
}

var serverError []byte

func init() {
	// Make sure there is always an error to return in case encoding fails
	e := ErrorMessage{
		Message: http.StatusText(http.StatusInternalServerError),
	}

	eb, err := json.Marshal(&e)
	if err != nil {
		panic(err)
	}
	serverError = eb
}

func EncodeError(err error) []byte {
	if err == nil {
		return nil
	}

	e := ErrorMessage{
		Message: err.Error(),
	}
	var apierr *Error
	if errors.As(err, &apierr) {
		e.Status = apierr.Status()
	}

	data, err := json.Marshal(&e)
	if err != nil {
		return serverError
	}
	return data
}

func DecodeError(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var e ErrorMessage
	err := json.Unmarshal(data, &e)
	if err != nil {
		return fmt.Errorf("cannot decode error message: %s", err)
	}

	apierr := NewError(errors.New(e.Message), e.Status)
	return errors.New(apierr.Text())
}
