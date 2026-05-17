package format

import "fmt"

func BytesSizePretty(size int64) string {
	neg := false
	if size < 0 {
		neg = true
		size = size * -1
	}

	str := UBytesSizePretty(uint64(size))

	if neg {
		return fmt.Sprintf("-%s", str)
	}
	return str
}

func UBytesSizePretty(size uint64) string {
	var str string
	switch {
	case size < 1024:
		str = fmt.Sprintf("%d B", size)
	case size < 1024*1024:
		str = fmt.Sprintf("%.1f KiB", float64(size)/1024)
	case size < 1024*1024*1024:
		str = fmt.Sprintf("%.1f MiB", float64(size)/1024/1024)
	case size < 1024*1024*1024*1024:
		str = fmt.Sprintf("%.1f GiB", float64(size)/1024/1024/1024)
	default:
		str = fmt.Sprintf("%.1f TiB", float64(size)/1024/1024/1024/1024)
	}

	return str
}

func CountPretty(n int64) string {
	neg := false
	if n < 0 {
		neg = true
		n = n * -1
	}

	str := UCountPretty(uint64(n))

	if neg {
		return fmt.Sprintf("-%s", str)
	}
	return str
}

func UCountPretty(n uint64) string {
	var str string
	switch {
	case n < 1000:
		str = fmt.Sprintf("%d", n)
	case n < 1000*1000:
		str = fmt.Sprintf("%.1fK", float64(n)/1000)
	default:
		str = fmt.Sprintf("%.1fM", float64(n)/1000/1000/1000)
	}
	return str
}
