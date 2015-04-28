package shp

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
)

// Reader provides a interface for reading Shapefiles. Calls
// to the Next method will iterate through the objects in the
// Shapefile. After a call to Next the object will be available
// through the Shape method.
type Reader struct {
	GeometryType ShapeType
	bbox         Box

	shp        *os.File
	shape      Shape
	num        int32
	filename   string
	filelength int32

	dbf             *os.File
	dbfFields       []Field
	dbfNumRecords   int32
	dbfHeaderLength int16
	dbfRecordLength int16

	errReader
}

type errReader struct {
	err error
}

func (er *errReader) littleRead(r io.Reader, data interface{}) {
	if er.err != nil {
		return
	}
	er.err = binary.Read(r, binary.LittleEndian, data)
}

func (er *errReader) bigRead(r io.Reader, data interface{}) {
	if er.err != nil {
		return
	}
	er.err = binary.Read(r, binary.BigEndian, data)
}

// Err returns the first non-EOF error that was encountered by the Reader.
func (r *Reader) Err() error {
	return r.err
}

// Open opens a Shapefile for reading.
func Open(filename string) (*Reader, error) {
	filename = filename[0 : len(filename)-3]
	shp, err := os.Open(filename + "shp")
	if err != nil {
		return nil, err
	}
	s := &Reader{filename: filename, shp: shp}
	s.readHeaders()
	return s, s.Err()
}

func (r *Reader) BBox() Box {
	return r.bbox
}

// Read and parse headers in the Shapefile. This will
// fill out GeometryType, filelength and bbox.
func (r *Reader) readHeaders() {
	var filecode int32
	r.bigRead(r.shp, &filecode)
	if r.err != nil {
		return
	}
	if filecode != 9994 {
		r.err = fmt.Errorf("invalid file code %d", filecode)
		return
	}

	r.shp.Seek(24, 0)
	// file length
	r.bigRead(r.shp, &r.filelength)
	// File length header is the number of 16-bit words, store byte counts.
	r.filelength *= 2

	var ver int32
	r.littleRead(r.shp, &ver)
	if r.err != nil {
		return
	}
	if ver != 1000 {
		r.err = fmt.Errorf("invalid file version %d", ver)
		return
	}

	r.shp.Seek(32, 0)
	r.littleRead(r.shp, &r.GeometryType)
	r.littleRead(r.shp, &r.bbox.MinX)
	r.littleRead(r.shp, &r.bbox.MinY)
	r.littleRead(r.shp, &r.bbox.MaxX)
	r.littleRead(r.shp, &r.bbox.MaxY)
	r.shp.Seek(100, 0)
}

// Close closes the Shapefile.
func (r *Reader) Close() {
	r.shp.Close()
	if r.dbf != nil {
		r.dbf.Close()
	}
}

// Shape returns the most recent feature that was read by
// a call to Next. It returns two values, the int is the
// object index starting from zero in the shapefile which
// can be used as row in ReadAttribute, and the Shape is the object.
func (r *Reader) Shape() (int, Shape) {
	return int(r.num) - 1, r.shape
}

// Next reads in the next Shape in the Shapefile, which will then be available
// through the Shape method. It returns false when the reader has reached the
// end of the file.  After Next returns false, the Err method will return any
// error that occurred during scanning, except that if it was io.EOF, Err will
// return nil.
func (r *Reader) Next() bool {
	cur, _ := r.shp.Seek(0, os.SEEK_CUR)
	if cur >= int64(r.filelength) {
		return false
	}

	var size int32
	var shapetype ShapeType
	r.bigRead(r.shp, &r.num)
	r.bigRead(r.shp, &size)
	r.littleRead(r.shp, &shapetype)
	if r.err != nil {
		return false
	}

	switch shapetype {
	case NULL:
		r.shape = new(Null)
	case POINT:
		r.shape = new(Point)
	case POLYLINE:
		r.shape = new(PolyLine)
	case POLYGON:
		r.shape = new(Polygon)
	case MULTIPOINT:
		r.shape = new(MultiPoint)
	case POINTZ:
		r.shape = new(PointZ)
	case POLYLINEZ:
		r.shape = new(PolyLineZ)
	case POLYGONZ:
		r.shape = new(PolygonZ)
	case MULTIPOINTZ:
		r.shape = new(MultiPointZ)
	case POINTM:
		r.shape = new(PointM)
	case POLYLINEM:
		r.shape = new(PolyLineM)
	case POLYGONM:
		r.shape = new(PolygonM)
	case MULTIPOINTM:
		r.shape = new(MultiPointM)
	case MULTIPATCH:
		r.shape = new(MultiPatch)
	default:
		log.Fatal("Unsupported shape type:", shapetype)
	}
	r.shape.read(r.shp)
	if r.Err() != nil {
		return false
	}

	// move to next object
	r.shp.Seek(int64(size)*2+cur+8, 0)
	return true
}

// Opens DBF file using r.filename + "dbf". This method
// will parse the header and fill out all dbf* values int
// the f object.
func (r *Reader) openDbf() (err error) {
	if r.dbf != nil {
		return
	}

	r.dbf, err = os.Open(r.filename + "dbf")
	if err != nil {
		return
	}

	// read header
	r.dbf.Seek(4, os.SEEK_SET)
	r.littleRead(r.dbf, &r.dbfNumRecords)
	r.littleRead(r.dbf, &r.dbfHeaderLength)
	r.littleRead(r.dbf, &r.dbfRecordLength)

	r.dbf.Seek(20, os.SEEK_CUR) // skip padding
	numFields := int(math.Floor(float64(r.dbfHeaderLength-33) / 32.0))
	r.dbfFields = make([]Field, numFields)
	r.littleRead(r.dbf, &r.dbfFields)

	return
}

// Fields returns a slice of Fields that are present in the
// DBF table.
func (r *Reader) Fields() []Field {
	r.openDbf() // make sure we have dbf file to read from
	return r.dbfFields
}

// AttributeCount returns number of records in the DBF table.
func (r *Reader) AttributeCount() int {
	r.openDbf() // make sure we have a dbf file to read from
	return int(r.dbfNumRecords)
}

// ReadAttribute returns the attribute value at row for field in
// the DBF table as a string. Both values starts at 0.
func (r *Reader) ReadAttribute(row int, field int) string {
	r.openDbf() // make sure we have a dbf file to read from
	seekTo := 1 + int64(r.dbfHeaderLength) + (int64(row) * int64(r.dbfRecordLength))
	for n := 0; n < field; n++ {
		seekTo += int64(r.dbfFields[n].Size)
	}
	r.dbf.Seek(seekTo, os.SEEK_SET)
	buf := make([]byte, r.dbfFields[field].Size)
	r.dbf.Read(buf)
	return strings.Trim(string(buf[:]), " ")
}
