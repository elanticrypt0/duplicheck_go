package models

import (
	"duplicheck/pkg/errors"

	"gorm.io/gorm"
)

type File struct {
	ID uint
	gorm.Model
	Name       string `json:"name"`
	Ext        string `json:"ext"`
	Filetype   string `json:"filetype"`
	Path       string `json:"path"`
	Size       uint   `json:"size"`
	Hash       string `json:"hash"`
	HasPreview bool   `json:"has_preview"`
	Tags       []Tag  `gorm:"many2many:file_tags;" json:"tags"`
}

func NewFile() *File {
	return &File{}
}

func (me *File) FindAllPagination(db *gorm.DB, itemsPerPage, currentPage int) (*[]File, error) {
	files := []File{}

	db.Order("created_at ASC").Limit(itemsPerPage).Offset(itemsPerPage * currentPage).Find(&files)
	if len(files) <= 0 {
		return nil, errors.Generic("code-0", "No files")
	}
	return &files, nil
}

func (me *File) ConvertToMap() map[string]interface{} {

	return map[string]interface{}{
		"name":       me.Name,
		"ext":        me.Ext,
		"filetype":   me.Filetype,
		"path":       me.Path,
		"size":       me.Size,
		"hash":       me.Hash,
		"hasPreview": me.HasPreview,
		"tags":       me.Tags,
	}

}

func (me *File) Create(db *gorm.DB, file File) error {

	if !me.IsFileInSameDir(db, file.Name, file.Path) {

		result := db.Create(&file)
		if result.Error != nil {
			return result.Error
		}
		return nil
	}
	return nil

}

func (me *File) IsHashfile(db *gorm.DB, hash string) bool {
	file := &File{}
	db.First(file, "hash = ?", hash)
	if file.ID == 0 {
		return false
	}

	return true
}

func (me *File) FindByHash(db *gorm.DB, hash string) []File {
	file := &[]File{}
	db.Find(file, "hash = ?", hash)
	return *file
}

func (me *File) IsFileInSameDir(db *gorm.DB, filename, fullpath string) bool {
	file := &File{}
	db.First(file, "name = ? and path = ?", filename, fullpath)
	if file.ID == 0 {
		return false
	}

	return true
}

func (me *File) FindDuplicateHashes(db *gorm.DB) ([]File, error) {
	var files = &[]File{}

	duplicateHashQuery := db.Model(&File{}).
		Select("hash").
		Group("hash").
		Having("COUNT(*) > 1")

	result := db.Where("hash in (?)", duplicateHashQuery).
		Order("hash").
		Find(&files)
	if result.Error != nil {
		return nil, result.Error
	}

	return *files, nil
}

func (me *File) CountDuplicateHashes(db *gorm.DB) (int, error) {
	var files = &[]File{}

	duplicateHashQuery := db.Model(&File{}).
		Select("hash").
		Group("hash").
		Having("COUNT(*) > 1")

	result := db.Where("hash in (?)", duplicateHashQuery).
		Order("hash").
		Find(&files)
	if result.Error != nil {
		return 0, result.Error
	}

	return len(*files), nil
}
