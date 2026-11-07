//go:build windows

package poolfile

import "context"

// Lock 在 Windows 上退化为无锁，调用方不得并发运行池文件操作。
func Lock(ctx context.Context, path string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return func() {}, nil
}
