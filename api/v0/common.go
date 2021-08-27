package v0

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type Error struct {
	Message string
}

var serverError []byte

func init() {
	// Make sure there is always an error to return in case encoding fails
	e := Error{
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
	errMsg := Error{
		Message: err.Error(),
	}
	data, err := json.Marshal(&errMsg)
	if err != nil {
		return serverError
	}
	return data
}

func DecodeError(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var e Error
	err := json.Unmarshal(data, &e)
	if err != nil {
		return fmt.Errorf("cannot decode error message: %s", err)
	}
	return errors.New(e.Message)
}

func MakeError(status int, err error) error {
	parts := make([]string, 0, 5)
	if status != 0 {
		parts = append(parts, fmt.Sprintf("%d", status))
		text := http.StatusText(status)
		if text != "" {
			parts = append(parts, " ")
			parts = append(parts, text)
		}
	}
	if err != nil {
		if len(parts) != 0 {
			parts = append(parts, ": ")
		}
		parts = append(parts, err.Error())
	}

	return errors.New(strings.Join(parts, ""))
}
