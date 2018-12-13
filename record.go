package kafka

import "bufio"

type recordHeader struct {
	Key   string
	Value []byte
}

func readRecordHeader(reader *bufio.Reader, remain int) (hdr recordHeader, err error) {
	var (
		keyLength   int64
		valueLength int64
	)

	if remain, err = readVarint(reader, remain, &keyLength); err != nil {
		return
	}

	if hdr.Key, remain, err = readNewString(reader, remain, int(keyLength)); err != nil {
		return
	}

	if remain, err = readVarint(reader, remain, &valueLength); err != nil {
		return
	}

	if remain, err = readBytes(reader, remain, &hdr.Value); err != nil {
		return
	}

	return
}

type record struct {
	Attributes  int8
	Timestamp   int64
	OffsetDelta int64
	Key         []byte
	Value       []byte
	Headers     []recordHeader
}

type recordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	MagicByte            int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []record
}

type recordBatchReader struct {
	reader *bufio.Reader
}

func newRecordBatchReader(reader *bufio.Reader) *recordBatchReader {
	return &recordBatchReader{
		reader: reader,
	}
}

func (r *recordBatchReader) readRecord(
	key func(*bufio.Reader, int, int) (int, error),
	val func(*bufio.Reader, int, int) (int, error),
) (offset int64, timestamp int64, err error) {

	panic("not implemented")

}
