package topic

type source struct {
	key string
}

type condAnd struct {
	source  Expr
	targets []Expr
}

type condOr struct {
	source  Expr
	targets []Expr
}

type condNot struct {
	source  Expr
	targets []Expr
}

// And returns Expr
func And(source Expr, targets ...Expr) Expr {
	if len(targets) == 0 {
		return source
	}

	var cond condAnd
	switch s := source.(type) {
	case condAnd:
		cond = s
	default:
		cond = condAnd{
			source: source,
		}
	}

	for _, target := range targets {
		switch t := target.(type) {
		case condAnd:
			cond.targets = append([]Expr{t.source}, t.targets...)
		default:
			cond.targets = append(cond.targets, t)
		}
	}

	return cond
}

// Or returns Expr
func Or(source Expr, targets ...Expr) Expr {
	if len(targets) == 0 {
		return source
	}

	var cond condOr
	switch s := source.(type) {
	case condOr:
		cond = s
	default:
		cond = condOr{
			source: source,
		}
	}

	for _, target := range targets {
		switch t := target.(type) {
		case condOr:
			cond.targets = append([]Expr{t.source}, t.targets...)
		default:
			cond.targets = append(cond.targets, t)
		}
	}

	return cond
}

// Not returns Expr
func Not(source Expr, targets ...Expr) Expr {
	if len(targets) == 0 {
		return source
	}

	var cond condNot
	switch s := source.(type) {
	case condNot:
		cond = s
	default:
		cond = condNot{
			source: source,
		}
	}

	for _, target := range targets {
		switch t := target.(type) {
		case condNot:
			cond.targets = append([]Expr{t.source}, t.targets...)
		default:
			cond.targets = append(cond.targets, t)
		}
	}

	return cond
}

// Source returns Expr from topic.
func Source(key string) Expr {
	return source{key}
}

func (e source) And(targets ...Expr) Expr {
	return And(e, targets...)
}

func (e source) Or(targets ...Expr) Expr {
	return Or(e, targets...)
}

func (e source) Not(targets ...Expr) Expr {
	return Not(e, targets...)
}

func (e source) Exec(p Pipeline) string {
	return p.Source(e.key)
}

func (e condAnd) And(targets ...Expr) Expr {
	return And(e, targets...)
}

func (e condAnd) Or(targets ...Expr) Expr {
	return Or(e, targets...)
}

func (e condAnd) Not(targets ...Expr) Expr {
	return Not(e, targets...)
}

func (e condAnd) Exec(pipe Pipeline) string {
	keys := make([]string, 0, len(e.targets)+1)
	keys = append(keys, e.source.Exec(pipe))
	for _, target := range e.targets {
		keys = append(keys, target.Exec(pipe))
	}

	dest := pipe.Session()
	pipe.Inter(dest, keys...)
	return dest
}

func (e condOr) And(targets ...Expr) Expr {
	return And(e, targets...)
}

func (e condOr) Or(targets ...Expr) Expr {
	return Or(e, targets...)
}

func (e condOr) Not(targets ...Expr) Expr {
	return Not(e, targets...)
}

func (e condOr) Exec(pipe Pipeline) string {
	keys := make([]string, 0, len(e.targets)+1)
	keys = append(keys, e.source.Exec(pipe))
	for _, target := range e.targets {
		keys = append(keys, target.Exec(pipe))
	}

	dest := pipe.Session()
	pipe.Union(dest, keys...)
	return dest
}

func (e condNot) And(targets ...Expr) Expr {
	return And(e, targets...)
}

func (e condNot) Or(targets ...Expr) Expr {
	return Or(e, targets...)
}

func (e condNot) Not(targets ...Expr) Expr {
	return Not(e, targets...)
}

func (e condNot) Exec(pipe Pipeline) string {
	keys := make([]string, 0, len(e.targets)+1)
	keys = append(keys, e.source.Exec(pipe))
	for _, target := range e.targets {
		keys = append(keys, target.Exec(pipe))
	}

	dest := pipe.Session()
	pipe.Diff(dest, keys...)
	return dest
}
