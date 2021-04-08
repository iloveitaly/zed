package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/brimdata/zed/compiler"
	"github.com/brimdata/zed/zbuf"
	"github.com/brimdata/zed/zio/tzngio"
	"github.com/brimdata/zed/zng"
	"github.com/brimdata/zed/zson"
	"github.com/stretchr/testify/assert"
)

type counter struct {
	n int
}

func (c *counter) Write(*zng.Record) error {
	c.n++
	return nil
}

func TestMuxDriver(t *testing.T) {
	input := `
#0:record[_path:string,ts:time]
0:[conn;1425565514.419939;]`

	query, err := compiler.ParseProc("split (=>tail 1 =>tail 1)")
	assert.NoError(t, err)

	t.Run("muxed into one writer", func(t *testing.T) {
		zctx := zson.NewContext()
		reader := tzngio.NewReader(strings.NewReader(input), zctx)
		assert.NoError(t, err)
		c := counter{}
		d := NewCLI(&c)
		err = Run(context.Background(), d, query, zctx, reader, Config{})
		assert.NoError(t, err)
		assert.Equal(t, 2, c.n)
	})

	t.Run("muxed into individual writers", func(t *testing.T) {
		zctx := zson.NewContext()
		reader := tzngio.NewReader(strings.NewReader(input), zctx)
		assert.NoError(t, err)
		cs := []zbuf.Writer{&counter{}, &counter{}}
		d := NewCLI(cs...)
		err = Run(context.Background(), d, query, zctx, reader, Config{})
		assert.NoError(t, err)
		assert.Equal(t, 1, cs[0].(*counter).n)
		assert.Equal(t, 1, cs[1].(*counter).n)
	})
}
