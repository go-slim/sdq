package libsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

// connector applies connection-local pragmas to every physical connection opened by
// database/sql. foreign_keys and synchronous are connection-local in SQLite-compatible
// databases, so configuring only the first pooled connection is not sufficient.
type connector struct {
	base driver.Connector
}

var _ driver.Connector = (*connector)(nil)

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}

	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.Join(
			errors.New("libsql: driver connection does not support ExecContext"),
			conn.Close(),
		)
	}

	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA synchronous = NORMAL",
	}
	for _, pragma := range pragmas {
		if _, err := execer.ExecContext(ctx, pragma, nil); err != nil {
			return nil, errors.Join(
				fmt.Errorf("libsql: configure connection with %q: %w", pragma, err),
				conn.Close(),
			)
		}
	}

	return conn, nil
}

func (c *connector) Driver() driver.Driver {
	return c.base.Driver()
}
