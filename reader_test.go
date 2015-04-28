package shp

import "testing"

func pointsEqual(t *testing.T, a, b []float64) {
	if len(a) != len(b) {
		t.Errorf("Points did not match, %v != %v", a, b)
		return
	}
	for k, v := range a {
		if v != b[k] {
			t.Errorf("Points did not match, %v != %v", a, b)
			return
		}
	}
}

func getShapes(filename string, t *testing.T) (shapes []Shape) {
	file, err := Open(filename)
	if err != nil {
		t.Fatal("Failed to open shapefile: " + filename + " (" + err.Error() + ")")
	}
	defer file.Close()

	for file.Next() {
		_, shape := file.Shape()
		shapes = append(shapes, shape)
	}
	if err := file.Err(); err != nil {
		t.Fatal(err)
	}
	return shapes
}

func test_Point(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*Point)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		pointsEqual(t, []float64{p.X, p.Y}, points[n])
	}
}

func test_PolyLine(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PolyLine)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y})
		}
	}
}

func test_Polygon(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*Polygon)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y})
		}
	}
}

func test_MultiPoint(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*MultiPoint)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y})
		}
	}
}

func test_PointZ(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PointZ)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		pointsEqual(t, []float64{p.X, p.Y, p.Z}, points[n])
	}
}

func test_PolyLineZ(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PolyLineZ)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.ZArray[k]})
		}
	}
}

func test_PolygonZ(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PolygonZ)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.ZArray[k]})
		}
	}
}

func test_MultiPointZ(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*MultiPointZ)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.ZArray[k]})
		}
	}
}

func test_PointM(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PointM)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		pointsEqual(t, []float64{p.X, p.Y, p.M}, points[n])
	}
}

func test_PolyLineM(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PolyLineM)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.MArray[k]})
		}
	}
}

func test_PolygonM(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*PolygonM)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.MArray[k]})
		}
	}
}

func test_MultiPointM(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*MultiPointM)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.MArray[k]})
		}
	}
}

func test_MultiPatch(t *testing.T, filename string, points [][]float64, shapes_num int) {
	shapes := getShapes(filename, t)
	if len(shapes) != shapes_num {
		t.Error("Number of shapes read was wrong.")
	}
	for n, s := range shapes {
		p, ok := s.(*MultiPatch)
		if !ok {
			t.Fatal("Failed to type assert.")
		}
		for k, point := range p.Points {
			pointsEqual(t, points[n*3+k], []float64{point.X, point.Y, p.ZArray[k]})
		}
	}
}

func TestReadBBox(t *testing.T) {
	tests := []struct {
		filename string
		want     Box
	}{
		{"test_files/multipatch.shp", Box{0, 0, 10, 10}},
		{"test_files/multipoint.shp", Box{0, 5, 10, 10}},
		{"test_files/multipointm.shp", Box{0, 5, 10, 10}},
		{"test_files/multipointz.shp", Box{0, 5, 10, 10}},
		{"test_files/point.shp", Box{0, 5, 10, 10}},
		{"test_files/pointm.shp", Box{0, 5, 10, 10}},
		{"test_files/pointz.shp", Box{0, 5, 10, 10}},
		{"test_files/polygon.shp", Box{0, 0, 5, 5}},
		{"test_files/polygonm.shp", Box{0, 0, 5, 5}},
		{"test_files/polygonz.shp", Box{0, 0, 5, 5}},
		{"test_files/polyline.shp", Box{0, 0, 25, 25}},
		{"test_files/polylinem.shp", Box{0, 0, 25, 25}},
		{"test_files/polylinez.shp", Box{0, 0, 25, 25}},
	}
	for _, tt := range tests {
		r, err := Open(tt.filename)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if got := r.BBox().MinX; got != tt.want.MinX {
			t.Errorf("got MinX = %v, want %v", got, tt.want.MinX)
		}
		if got := r.BBox().MinY; got != tt.want.MinY {
			t.Errorf("got MinY = %v, want %v", got, tt.want.MinY)
		}
		if got := r.BBox().MaxX; got != tt.want.MaxX {
			t.Errorf("got MaxX = %v, want %v", got, tt.want.MaxX)
		}
		if got := r.BBox().MaxY; got != tt.want.MaxY {
			t.Errorf("got MaxY = %v, want %v", got, tt.want.MaxY)
		}
	}
}

func TestReadPoint(t *testing.T) {
	points := [][]float64{
		{10, 10},
		{5, 5},
		{0, 10},
	}
	test_Point(t, "test_files/point.shp", points, 3)
}

func TestReadPolyLine(t *testing.T) {
	points := [][]float64{
		{0, 0},
		{5, 5},
		{10, 10},
		{15, 15},
		{20, 20},
		{25, 25},
	}
	test_PolyLine(t, "test_files/polyline.shp", points, 2)
}

func TestReadPolygon(t *testing.T) {
	points := [][]float64{
		{0, 0},
		{0, 5},
		{5, 5},
		{5, 0},
		{0, 0},
	}
	test_Polygon(t, "test_files/polygon.shp", points, 1)
}

func TestReadMultiPoint(t *testing.T) {
	points := [][]float64{
		{10, 10},
		{5, 5},
		{0, 10},
	}
	test_MultiPoint(t, "test_files/multipoint.shp", points, 1)
}

func TestReadPointZ(t *testing.T) {
	points := [][]float64{
		{10, 10, 100},
		{5, 5, 50},
		{0, 10, 75},
	}
	test_PointZ(t, "test_files/pointz.shp", points, 3)
}

func TestReadPolyLineZ(t *testing.T) {
	points := [][]float64{
		{0, 0, 0},
		{5, 5, 5},
		{10, 10, 10},
		{15, 15, 15},
		{20, 20, 20},
		{25, 25, 25},
	}
	test_PolyLineZ(t, "test_files/polylinez.shp", points, 2)
}

func TestReadPolygonZ(t *testing.T) {
	points := [][]float64{
		{0, 0, 0},
		{0, 5, 5},
		{5, 5, 10},
		{5, 0, 15},
		{0, 0, 0},
	}
	test_PolygonZ(t, "test_files/polygonz.shp", points, 1)
}

func TestReadMultiPointZ(t *testing.T) {
	points := [][]float64{
		{10, 10, 100},
		{5, 5, 50},
		{0, 10, 75},
	}
	test_MultiPointZ(t, "test_files/multipointz.shp", points, 1)
}

func TestReadPointM(t *testing.T) {
	points := [][]float64{
		{10, 10, 100},
		{5, 5, 50},
		{0, 10, 75},
	}
	test_PointM(t, "test_files/pointm.shp", points, 3)
}

func TestReadPolyLineM(t *testing.T) {
	points := [][]float64{
		{0, 0, 0},
		{5, 5, 5},
		{10, 10, 10},
		{15, 15, 15},
		{20, 20, 20},
		{25, 25, 25},
	}
	test_PolyLineM(t, "test_files/polylinem.shp", points, 2)
}

func TestReadPolygonM(t *testing.T) {
	points := [][]float64{
		{0, 0, 0},
		{0, 5, 5},
		{5, 5, 10},
		{5, 0, 15},
		{0, 0, 0},
	}
	test_PolygonM(t, "test_files/polygonm.shp", points, 1)
}

func TestReadMultiPointM(t *testing.T) {
	points := [][]float64{
		{10, 10, 100},
		{5, 5, 50},
		{0, 10, 75},
	}
	test_MultiPointM(t, "test_files/multipointm.shp", points, 1)
}

func TestReadMultipatch(t *testing.T) {
	points := [][]float64{
		{0, 0, 0},
		{10, 0, 0},
		{10, 10, 0},
		{0, 10, 0},
		{0, 0, 0},
		{0, 10, 0},
		{0, 10, 10},
		{0, 0, 10},
		{0, 0, 0},
		{0, 10, 0},
		{10, 0, 0},
		{10, 0, 10},
		{10, 10, 10},
		{10, 10, 0},
		{10, 0, 0},
		{0, 0, 0},
		{0, 0, 10},
		{10, 0, 10},
		{10, 0, 0},
		{0, 0, 0},
		{10, 10, 0},
		{10, 10, 10},
		{0, 10, 10},
		{0, 10, 0},
		{10, 10, 0},
		{0, 0, 10},
		{0, 10, 10},
		{10, 10, 10},
		{10, 0, 10},
		{0, 0, 10},
	}
	test_MultiPatch(t, "test_files/multipatch.shp", points, 1)
}
