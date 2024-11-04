// Copyright 2022 The Sqlite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package functest // modernc.org/sqlite/functest

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlite3 "modernc.org/sqlite"
)

var finalCalled bool

type sumFunction struct {
	sum int64
	finalCalled *bool
}

func (f *sumFunction) Step(ctx *sqlite3.FunctionContext, args []driver.Value) error {
	switch resTyped := args[0].(type) {
	case int64:
		f.sum += resTyped
	default:
		return fmt.Errorf("function did not return a valid driver.Value: %T", resTyped)
	}
	return nil
}

func (f *sumFunction) WindowInverse(ctx *sqlite3.FunctionContext, args []driver.Value) error {
	switch resTyped := args[0].(type) {
	case int64:
		f.sum -= resTyped
	default:
		return fmt.Errorf("function did not return a valid driver.Value: %T", resTyped)
	}
	return nil
}

func (f *sumFunction) WindowValue(ctx *sqlite3.FunctionContext) (driver.Value, error) {
	return f.sum, nil
}

func (f *sumFunction) Final(ctx *sqlite3.FunctionContext) {
	*f.finalCalled = true
}

func init() {
	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_int64",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return int64(42), nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_float64",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return float64(1e-2), nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_null",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return nil, nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_error",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return nil, errors.New("boom")
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_empty_byte_slice",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return []byte{}, nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_nonempty_byte_slice",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return []byte("abcdefg"), nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_empty_string",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return "", nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"test_nonempty_string",
		0,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			return "abcdefg", nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"yesterday",
		1,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			var arg time.Time
			switch argTyped := args[0].(type) {
			case int64:
				arg = time.Unix(argTyped, 0)
			default:
				fmt.Println(argTyped)
				return nil, fmt.Errorf("expected argument to be int64, got: %T", argTyped)
			}
			return arg.Add(-24 * time.Hour), nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"md5",
		1,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			var arg *bytes.Buffer
			switch argTyped := args[0].(type) {
			case string:
				arg = bytes.NewBuffer([]byte(argTyped))
			case []byte:
				arg = bytes.NewBuffer(argTyped)
			default:
				return nil, fmt.Errorf("expected argument to be a string, got: %T", argTyped)
			}
			w := md5.New()
			if _, err := arg.WriteTo(w); err != nil {
				return nil, fmt.Errorf("unable to compute md5 checksum: %s", err)
			}
			return hex.EncodeToString(w.Sum(nil)), nil
		},
	)

	sqlite3.MustRegisterDeterministicScalarFunction(
		"regexp",
		2,
		func(ctx *sqlite3.FunctionContext, args []driver.Value) (driver.Value, error) {
			var s1 string
			var s2 string

			switch arg0 := args[0].(type) {
			case string:
				s1 = arg0
			default:
				return nil, errors.New("expected argv[0] to be text")
			}

			fmt.Println(args)

			switch arg1 := args[1].(type) {
			case string:
				s2 = arg1
			default:
				return nil, errors.New("expected argv[1] to be text")
			}

			matched, err := regexp.MatchString(s1, s2)
			if err != nil {
				return nil, fmt.Errorf("bad regular expression: %q", err)
			}

			return matched, nil
		},
	)

	sqlite3.MustRegisterFunction("test_sum", &sqlite3.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		MakeAggregate: func(ctx sqlite3.FunctionContext) (sqlite3.AggregateFunction, error) {
			return &sumFunction{finalCalled: &finalCalled}, nil
		},
	})

	sqlite3.MustRegisterFunction("test_aggregate_error", &sqlite3.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		MakeAggregate: func(ctx sqlite3.FunctionContext) (sqlite3.AggregateFunction, error) {
			return nil, errors.New("boom")
		},
	})

	sqlite3.MustRegisterFunction("test_aggregate_null_pointer", &sqlite3.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		MakeAggregate: func(ctx sqlite3.FunctionContext) (sqlite3.AggregateFunction, error) {
			return nil, nil
		},
	})
}

func TestRegisteredFunctions(t *testing.T) {
	withDB := func(test func(db *sql.DB)) {
		db, err := sql.Open("sqlite", "file::memory:")
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()

		finalCalled = false
		test(db)
	}

	t.Run("int64", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_int64()")

			var a int
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := a, 42; g != e {
				tt.Fatal(g, e)
			}

		})
	})

	t.Run("float64", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_float64()")

			var a float64
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := a, 1e-2; g != e {
				tt.Fatal(g, e)
			}

		})
	})

	t.Run("error", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			_, err := db.Query("select test_error()")
			if err == nil {
				tt.Fatal("expected error, got none")
			}
			if !strings.Contains(err.Error(), "boom") {
				tt.Fatal(err)
			}
		})
	})

	t.Run("empty_byte_slice", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_empty_byte_slice()")

			var a []byte
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if len(a) > 0 {
				tt.Fatal("expected empty byte slice")
			}
		})
	})

	t.Run("nonempty_byte_slice", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_nonempty_byte_slice()")

			var a []byte
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := a, []byte("abcdefg"); !bytes.Equal(g, e) {
				tt.Fatal(string(g), string(e))
			}
		})
	})

	t.Run("empty_string", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_empty_string()")

			var a string
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if len(a) > 0 {
				tt.Fatal("expected empty string")
			}
		})
	})

	t.Run("nonempty_string", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_nonempty_string()")

			var a string
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := a, "abcdefg"; g != e {
				tt.Fatal(g, e)
			}
		})
	})

	t.Run("null", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_null()")

			var a interface{}
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if a != nil {
				tt.Fatal("expected nil")
			}
		})
	})

	t.Run("dates", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select yesterday(unixepoch('2018-11-01'))")

			var a int64
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := time.Unix(a, 0), time.Date(2018, time.October, 31, 0, 0, 0, 0, time.UTC); !g.Equal(e) {
				tt.Fatal(g, e)
			}
		})
	})

	t.Run("md5", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select md5('abcdefg')")

			var a string
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := a, "7ac66c0f148de9519b8bd264312c4d64"; g != e {
				tt.Fatal(g, e)
			}
		})
	})

	t.Run("md5 with blob input", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			if _, err := db.Exec("create table t(b blob); insert into t values (?)", []byte("abcdefg")); err != nil {
				tt.Fatal(err)
			}
			row := db.QueryRow("select md5(b) from t")

			var a []byte
			if err := row.Scan(&a); err != nil {
				tt.Fatal(err)
			}
			if g, e := a, []byte("7ac66c0f148de9519b8bd264312c4d64"); !bytes.Equal(g, e) {
				tt.Fatal(string(g), string(e))
			}
		})
	})

	t.Run("regexp filter", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			t1 := "seafood"
			t2 := "fruit"

			if _, err := db.Exec("create table t(b text); insert into t values (?), (?)", t1, t2); err != nil {
				tt.Fatal(err)
			}
			rows, err := db.Query("select * from t where b regexp 'foo.*'")
			if err != nil {
				tt.Fatal(err)
			}

			type rec struct {
				b string
			}
			var a []rec
			for rows.Next() {
				var r rec
				if err := rows.Scan(&r.b); err != nil {
					tt.Fatal(err)
				}

				a = append(a, r)
			}
			if err := rows.Err(); err != nil {
				tt.Fatal(err)
			}

			if g, e := len(a), 1; g != e {
				tt.Fatal(g, e)
			}

			if g, e := a[0].b, t1; g != e {
				tt.Fatal(g, e)
			}
		})
	})

	t.Run("regexp matches", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select 'seafood' regexp 'foo.*'")

			var r int
			if err := row.Scan(&r); err != nil {
				tt.Fatal(err)
			}

			if g, e := r, 1; g != e {
				tt.Fatal(g, e)
			}
		})
	})

	t.Run("regexp does not match", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select 'fruit' regexp 'foo.*'")

			var r int
			if err := row.Scan(&r); err != nil {
				tt.Fatal(err)
			}

			if g, e := r, 0; g != e {
				tt.Fatal(g, e)
			}
		})
	})

	t.Run("regexp errors on bad regexp", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			_, err := db.Query("select 'seafood' regexp 'a(b'")
			if err == nil {
				tt.Fatal(errors.New("expected error, got none"))
			}
		})
	})

	t.Run("regexp errors on bad first argument", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			_, err := db.Query("SELECT 1 REGEXP 'a(b'")
			if err == nil {
				tt.Fatal(errors.New("expected error, got none"))
			}
		})
	})

	t.Run("regexp errors on bad second argument", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			_, err := db.Query("SELECT 'seafood' REGEXP 1")
			if err == nil {
				tt.Fatal(errors.New("expected error, got none"))
			}
		})
	})

	t.Run("sumFunction type error", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_sum('foo');")

			err := row.Scan()
			if err == nil {
				tt.Fatal("expected error, got none")
			}
			if !strings.Contains(err.Error(), "string") {
				tt.Fatal(err)
			}
			if !finalCalled {
				t.Error("xFinal not called")
			}
		})
	})

	t.Run("sumFunction multiple columns", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			if _, err := db.Exec("create table t(a int64, b int64); insert into t values (1, 5), (2, 6), (3, 7), (4, 8)"); err != nil {
				tt.Fatal(err)
			}
			row := db.QueryRow("select test_sum(a), test_sum(b) from t;")

			var a, b int64
			var e int64 = 10
			var f int64 = 26
			if err := row.Scan(&a, &b); err != nil {
				tt.Fatal(err)
			}
			if a != e {
				tt.Fatal(a, e)
			}
			if b != f {
				tt.Fatal(b, f)
			}
			if !finalCalled {
				t.Error("xFinal not called")
			}
		})
	})

	// https://www.sqlite.org/windowfunctions.html#user_defined_aggregate_window_functions
	t.Run("sumFunction as window function", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			if _, err := db.Exec("create table t3(x, y); insert into t3 values('a', 4), ('b', 5), ('c', 3), ('d', 8), ('e', 1);"); err != nil {
				tt.Fatal(err)
			}
			rows, err := db.Query("select x, test_sum(y) over (order by x rows between 1 preceding and 1 following) as sum_y from t3 order by x;")
			if err != nil {
				tt.Fatal(err)
			}
			defer rows.Close()

			type row struct {
				x    string
				sumY int64
			}

			got := make([]row, 0)
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.x, &r.sumY); err != nil {
					tt.Fatal(err)
				}
				got = append(got, r)
			}

			want := []row{
				{"a", 9},
				{"b", 12},
				{"c", 16},
				{"d", 12},
				{"e", 9},
			}

			if len(got) != len(want) {
				tt.Fatal(len(got), len(want))
			}

			for i, g := range got {
				if g != want[i] {
					tt.Fatal(i, g, want[i])
				}
			}
			if !finalCalled {
				t.Error("xFinal not called")
			}
		})
	})

	t.Run("aggregate function cannot be created", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_aggregate_error(1);")

			err := row.Scan()
			if err == nil {
				tt.Fatal("expected error, got none")
			}
			if !strings.Contains(err.Error(), "boom") {
				tt.Fatal(err)
			}
		})
	})

	t.Run("null aggregate function pointer", func(tt *testing.T) {
		withDB(func(db *sql.DB) {
			row := db.QueryRow("select test_aggregate_null_pointer(1);")

			err := row.Scan()
			if err == nil {
				tt.Fatal("expected error, got none")
			}
			if !strings.Contains(err.Error(), "MakeAggregate function returned nil") {
				tt.Fatal(err)
			}
		})
	})
}
