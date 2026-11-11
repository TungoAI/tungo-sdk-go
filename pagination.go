package tungo

import "iter"

// pager is implemented by the generated cursor-page types.
type pager[T any] interface {
	items() []T
	cursor() string
	HasNext() bool
}

// autoPage iterates over the items of every page. The next page is requested only when the
// consumer has used up the current one; leaving the loop stops fetching.
func autoPage[T any, P pager[T]](first func() (P, error), next func(P) (P, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		page, err := first()
		if err != nil {
			yield(zero, err)
			return
		}
		seen := map[string]bool{}
		for {
			for _, item := range page.items() {
				if !yield(item, nil) {
					return
				}
			}
			if !page.HasNext() {
				return
			}
			cursor := page.cursor()
			if seen[cursor] {
				yield(zero, &PaginationError{Reason: "the API returned a cursor that was already used; stopping instead of looping forever", Cursor: cursor})
				return
			}
			seen[cursor] = true
			page, err = next(page)
			if err != nil {
				yield(zero, err)
				return
			}
		}
	}
}
