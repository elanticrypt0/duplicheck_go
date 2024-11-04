package pagination

import "strconv"

type Pagination struct {
	PageFirst    uint `json:"pagefirst"`
	PageLast     uint `json:"pagelast"`
	PageCurrent  uint `json:"pagecurrent"`
	PageNext     uint `json:"pagenext"`
	PagePrev     uint `json:"pageprev"`
	ItemsPerPage uint `json:"itemsperpage"`
	TotalItems   uint `json:"totalitems"`
}

func NewPagination(currentPage, itemsPerPage, totalItems uint) *Pagination {
	p := &Pagination{
		PageFirst:    1,
		ItemsPerPage: itemsPerPage,
		PageCurrent:  currentPage,
		TotalItems:   totalItems,
	}
	p.calculatePages(totalItems)
	if currentPage+1 <= p.PageLast {
		p.PageNext++
	}

	if currentPage-1 > p.PageFirst {
		p.PagePrev--
	}
	return p
}

func (p *Pagination) calculatePages(totalItems uint) {
	if totalItems >= p.ItemsPerPage {
		pages := totalItems / p.ItemsPerPage
		pagesMod := totalItems % p.ItemsPerPage
		if pagesMod > p.ItemsPerPage {
			pages++
		}
		p.PageLast = pages
	} else {
		p.PageLast = 1
	}

}

func (p *Pagination) ToString(elem string) string {
	var val string
	switch elem {
	case "end":
		val = strconv.Itoa(int((p.PageLast)))
	case "start":
		val = strconv.Itoa(int((p.PageFirst)))
	case "next":
		val = strconv.Itoa(int((p.PageNext)))
	case "prev":
		val = strconv.Itoa(int((p.PagePrev)))
	case "current":
		val = strconv.Itoa(int((p.PagePrev)))
	}
	return val
}
