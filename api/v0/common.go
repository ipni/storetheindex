package v0

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/filecoin-project/storetheindex/internal/syserr"
)

type Error struct {
	Message string `json:",omitempty"`
	Status  int    `json:",omitempty"`
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

	e := Error{
		Message: err.Error(),
	}
	var se *syserr.SysError
	if errors.As(err, &se) {
		e.Status = se.Status()
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
	var e Error
	err := json.Unmarshal(data, &e)
	if err != nil {
		return fmt.Errorf("cannot decode error message: %s", err)
	}

	se := syserr.New(errors.New(e.Message), e.Status)
	return errors.New(se.Text())
}
