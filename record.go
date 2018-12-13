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

	if hdr.Value, remain, err = readNewBytes(reader, remain, int(valueLength)); err != nil {
		return
	}

	return
}

type record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []recordHeader
}

func readRecord(reader *bufio.Reader, remain int) (rec record, err error) {
	if remain, err = readInt8(reader, remain, &rec.Attributes); err != nil {
		return
	}

	if remain, err = readVarint(reader, remain, &rec.TimestampDelta); err != nil {
		return
	}

	if remain, err = readVarint(reader, remain, &rec.OffsetDelta); err != nil {
		return
	}

	var (
		keyLength   int64
		valueLength int64
	)

	if remain, err = readVarint(reader, remain, &keyLength); err != nil {
		return
	}

	if rec.Key, remain, err = readNewBytes(reader, remain, int(keyLength)); err != nil {
		return
	}

	if remain, err = readVarint(reader, remain, &valueLength); err != nil {
		return
	}

	if rec.Value, remain, err = readNewBytes(reader, remain, int(valueLength)); err != nil {
		return
	}

	remain, err = readArrayWith(reader, remain, func(reader *bufio.Reader, sz int) (int, error) {
		hdr, err := readRecordHeader(reader, sz)
		if err != nil {
			return sz, err
		}

		rec.Headers = append(rec.Headers, hdr)

		return nil
	})

	return
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
