package clause

type Writer interface {
	WriteByte(byte) error
	WriteString(string) (int, error)
}
