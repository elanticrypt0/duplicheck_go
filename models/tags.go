package models

import (
	"strings"

	"gorm.io/gorm"
)

type Tag struct {
	ID uint
	gorm.Model
	Name string `json:"name"`
	Slug string `json:"slug"`
}

func NewTag() Tag {
	return Tag{}
}

func (me *Tag) CreateOrFind(db *gorm.DB, tagname string) Tag {
	tagAux := &Tag{}
	if !me.exists(db, tagname) {
		tagAux.Name = tagname
		tagAux.Slug = me.makeSlug(tagname)
		db.Create(&tagAux)
	} else {
		db.First(&tagAux, "name = ?", tagname)
	}

	return *tagAux
}

func (me *Tag) exists(db *gorm.DB, tagname string) bool {
	tagAux := &Tag{}
	db.First(&tagAux, "name = ?", tagname)

	if tagAux.ID != 0 {
		return true
	}

	return false

}

func (me *Tag) makeSlug(tagname string) string {
	slug := strings.TrimSpace(tagname)
	slug = strings.ReplaceAll(slug, " ", "-")
	slug = strings.ToLower(slug)
	return slug

}
