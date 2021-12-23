package supervisor

import (
	"github.com/abrander/go-supervisord"
)

func New(addr string) (*supervisord.Client, error) {
	client, err := supervisord.NewClient(addr)
	if err != nil {
		return nil, err
	}
	return client, nil
}
