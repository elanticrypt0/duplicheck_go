package utils

import "strings"

var VideoExtensions = []string{"mp4", "mkv", "avi", "mov", "wmv", "m4v", "flv", "webm", "mpeg", "3gp", "m4p", "m2ts", "mts", "vob", "ogv"}

var ImageExtensions = []string{"jpg", "jpeg", "png", "gif", "bmp", "tiff", "svg", "webp", "ico"}

var DocumentExtensions = []string{"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "rtf", "odt", "ods", "odp", "csv", "tsv", "epub", "md"}

func IsVideoFile(extension string) bool {
	for _, ext := range VideoExtensions {
		if strings.EqualFold(extension, ext) {
			return true
		}
	}
	return false
}

func IsImageFile(extension string) bool {
	for _, ext := range ImageExtensions {
		if strings.EqualFold(extension, ext) {
			return true
		}
	}
	return false
}

func IsDocumentFile(extension string) bool {
	for _, ext := range DocumentExtensions {
		if strings.EqualFold(extension, ext) {
			return true
		}
	}
	return false
}

func GetFiletype(ext string) string {
	ftype := "other"

	if IsVideoFile(ext) {
		return "video"
	}

	if IsImageFile(ext) {
		return "image"
	}

	if IsDocumentFile(ext) {
		return "document"
	}

	return ftype
}
