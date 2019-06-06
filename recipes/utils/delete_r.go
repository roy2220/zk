package utils

import (
	"context"

	"github.com/let-z-go/zk"
)

func DeleteR(client *zk.Client, context_ context.Context, path string) error {
	path = client.NormalizePath(path)
	var doDeleteR func(path string) error

	doDeleteR = func(path string) error {
		for {
			e := client.Delete(context_, path, -1, true)

			if e == nil {
				return nil
			}

			e2, ok := e.(*zk.Error)

			if !ok {
				return e
			}

			switch e2.GetCode() {
			case zk.ErrorNoNode:
				return nil
			case zk.ErrorNotEmpty:
				response, e := client.GetChildren(context_, path, true)

				if e != nil {
					if e2, ok := e.(*zk.Error); ok && e2.GetCode() == zk.ErrorNoNode {
						return nil
					} else {
						return e
					}
				}

				for _, child := range response.Children {
					if e := doDeleteR(path + "/" + child); e != nil {
						return e
					}
				}
			default:
				return e
			}
		}
	}

	return doDeleteR(path)
}
