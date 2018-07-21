package utils

import (
	"context"

	"github.com/let-z-go/zk"
)

func CreateP(client *zk.Client, context_ context.Context, path string) error {
	path = client.NormalizePath(path)
	var doCreateP func(path string) error

	doCreateP = func(path string) error {
		for {
			_, e := client.Create(context_, path, []byte{}, nil, zk.CreatePersistent, true)

			if e == nil {
				return nil
			}

			e2, ok := e.(zk.Error)

			if !ok {
				return e
			}

			switch e2.GetCode() {
			case zk.ErrorNodeExists:
				return nil
			case zk.ErrorNoNode:
				parentPath := GetNodeParentPath(path)

				if e := doCreateP(parentPath); e != nil {
					return e
				}
			default:
				return e
			}
		}
	}

	return doCreateP(path)
}
