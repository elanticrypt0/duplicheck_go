package models

import "duplicheck/pkg/pagination"

type PaginatedFiles struct {
	Paginate pagination.Pagination `json:"paginate" param:"paginate" query:"paginate" form:"paginate"`
	Files    []File                `json:"files" param:"files" query:"files" form:"files"`
}
