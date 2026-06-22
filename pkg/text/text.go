package text

import "strings"

func Split(text string, width int) string {
	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}

	var builder strings.Builder
	currentLine := ""

	for _, word := range words {
		if currentLine == "" {
			currentLine = word
			continue
		}

		if len(currentLine)+1+len(word) <= width {
			currentLine += " " + word
		} else {
			builder.WriteString(currentLine)
			builder.WriteByte('\n')
			currentLine = word
		}
	}

	if currentLine != "" {
		builder.WriteString(currentLine)
	}

	return builder.String()
}

func FirstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
