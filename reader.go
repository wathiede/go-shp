package goshp

import (
	"encoding/binary"
	"log"
	"math"
	"os"
	"strings"
)

type Reader struct {
	filename     string
	shp          *os.File
	filelength   int64
	GeometryType ShapeType

	Fields          []Field
	dbf             *os.File
	dbfNumRecords   int32
	dbfHeaderLength int16
	dbfRecordLength int16
}

// Opens a Shapefile for reading.
func Open(filename string) (*Reader, error) {
	filename = filename[0 : len(filename)-3]
	shp, err := os.Open(filename + "shp")
	if err != nil {
		return nil, err
	}
	s := &Reader{filename: filename, shp: shp}
	s.readHeaders()
	s.openDbf()
	return s, nil
}

// Read and parse headers in the Shapefile. This will
// fill out GeometryType and filelength.
func (r *Reader) readHeaders() {
	// don't trust the the filelength in the header
	r.filelength, _ = r.shp.Seek(0, os.SEEK_END)

	var filelength int32
	r.shp.Seek(24, 0)
	// file length
	binary.Read(r.shp, binary.BigEndian, &filelength)
	r.shp.Seek(32, 0)
	binary.Read(r.shp, binary.LittleEndian, &r.GeometryType)
	r.shp.Seek(100, 0)
}

// Returns true if the file cursor has passed the end
// of the file.
func (r *Reader) EOF() (ok bool) {
	n, _ := r.shp.Seek(0, os.SEEK_CUR)
	if n >= r.filelength {
		ok = true
	}
	return
}

// Closes the Shapefile
func (r *Reader) Close() {
	r.shp.Close()
	if r.dbf != nil {
		r.dbf.Close()
	}
}

// Read and returns the next shape in the Shapefile as
// a Shape interface which can be type asserted to the
// correct type.
func (r *Reader) ReadShape() (shape Shape, err error) {
	var size int32
	var num int32
	var shapetype ShapeType
	binary.Read(r.shp, binary.BigEndian, &num)
	binary.Read(r.shp, binary.BigEndian, &size)
	cur, _ := r.shp.Seek(0, os.SEEK_CUR)
	binary.Read(r.shp, binary.LittleEndian, &shapetype)

	switch shapetype {
	case NULL:
		shape = new(Null)
	case POINT:
		shape = new(Point)
	case POLYLINE:
		shape = new(PolyLine)
	case POLYGON:
		shape = new(Polygon)
	case MULTIPOINT:
		shape = new(MultiPoint)
	case POINTZ:
		shape = new(PointZ)
	case POLYLINEZ:
		shape = new(PolyLineZ)
	case POLYGONZ:
		shape = new(PolygonZ)
	case MULTIPOINTZ:
		shape = new(MultiPointZ)
	case POINTM:
		shape = new(PointM)
	case POLYLINEM:
		shape = new(PolyLineM)
	case POLYGONM:
		shape = new(PolygonM)
	case MULTIPOINTM:
		shape = new(MultiPointM)
	case MULTIPATCH:
		shape = new(MultiPatch)
	default:
		log.Fatal("Unsupported shape type:", shapetype)
	}
	shape.read(r.shp)

	_, err = r.shp.Seek(int64(size)*2+cur, 0)
	return shape, err
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
	binary.Read(r.dbf, binary.LittleEndian, &r.dbfNumRecords)
	binary.Read(r.dbf, binary.LittleEndian, &r.dbfHeaderLength)
	binary.Read(r.dbf, binary.LittleEndian, &r.dbfRecordLength)

	r.dbf.Seek(20, os.SEEK_CUR) // skip padding
	numFields := int(math.Floor(float64(r.dbfHeaderLength-33) / 32.0))
	r.Fields = make([]Field, numFields)
	binary.Read(r.dbf, binary.LittleEndian, &r.Fields)

	return
}

// Returns number of records in the DBF table
func (r *Reader) AttributeCount() int {
	r.openDbf() // make sure we have a dbf file to read from
	return int(r.dbfNumRecords)
}

// Read attribute from DBF at row and field
func (r *Reader) ReadAttribute(row int, field int) string {
	r.openDbf() // make sure we have a dbf file to read from
	seekTo := 1 + int64(r.dbfHeaderLength) + (int64(row) * int64(r.dbfRecordLength))
	for n := 0; n < field; n++ {
		seekTo += int64(r.Fields[n].Size)
	}
	r.dbf.Seek(seekTo, os.SEEK_SET)
	buf := make([]byte, r.Fields[field].Size)
	r.dbf.Read(buf)
	return strings.Trim(string(buf[:]), " ")
}
