package cube

import (
	"errors"
	"fmt"
)

// Cube is an OLAP cube
type Cube struct {
	Dimensions []string        `json:"dimensions,omitempty"`
	Points     [][]interface{} `json:"points,omitempty"`
	Fields     []string        `json:"fields,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
}

// AddRows add a set of rows to the cube.
func (c *Cube) AddRows(header []string, rows [][]interface{}) error {

	if len(header) != (len(c.Dimensions) + len(c.Fields)) {
		return errors.New("invalid header")
	}

	dimIndexes := make([]int, len(c.Dimensions))
	for i, d := range c.Dimensions {

		found := false
		for j, h := range header {
			if d == h {

				dimIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			return errors.New("dimension not found")
		}
	}

	fldIndexes := make([]int, len(c.Fields))
	for i, f := range c.Fields {
		found := false
		for j, h := range header {
			if f == h {
				fldIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			return errors.New("dimension not found")
		}
	}

	for _, row := range rows {
		point := make([]interface{}, len(c.Dimensions))
		data := make([]interface{}, len(c.Fields))
		for i := range point {
			point[i] = row[dimIndexes[i]]
		}
		for i := range data {
			data[i] = row[fldIndexes[i]]
		}
		c.Points = append(c.Points, point)
		c.Data = append(c.Data, data)
	}

	return nil
}

func (c Cube) Slice(dimension string, value interface{}) Cube {

	newCube := Cube{}

	// Dimensions
	dimIndex := indexOf(dimension, c.Dimensions)
	newCube.Dimensions = sliceStringArray(c.Dimensions, dimIndex)

	// Fields
	newCube.Fields = copyStringArray(c.Fields)

	// Points + Data
	for i, pt := range c.Points {
		if pt[dimIndex] == value {

			newPt := sliceInterfaceArray(pt, dimIndex)
			newData := copyInterfaceArray(c.Data[i])

			newCube.Points = append(newCube.Points, newPt)
			newCube.Data = append(newCube.Data, newData)
		}
	}

	return newCube
}

func (c Cube) RollUp(dimensions []string, fields []string, aggregator Aggregator, initialValue []interface{}) Cube {

	newCube := Cube{}
	newCube.Dimensions = dimensions
	newCube.Fields = copyStringArray(fields)

	dimIndexes := make([]int, 0, len(dimensions))
	for _, dimension := range dimensions {
		dimIndex := indexOf(dimension, c.Dimensions)

		dimIndexes = append(dimIndexes, dimIndex)
	}

	mapKeyIndex := make(map[string]int)

	for i, originalPoint := range c.Points {

		newPt := make([]interface{}, 0, len(dimensions))
		for _, dimIndex := range dimIndexes {
			k := originalPoint[dimIndex]
			newPt = append(newPt, k)
		}

		k := key(newPt)
		idx, found := mapKeyIndex[k]
		if !found {
			idx = len(newCube.Points)
			mapKeyIndex[k] = idx
			newCube.Points = append(newCube.Points, newPt)
			newCube.Data = append(newCube.Data, copyInterfaceArray(initialValue))
		}

		value := newCube.Data[idx]

		newValue := aggregator(value, c.Data[i])

		newCube.Data[idx] = newValue
	}

	return newCube
}

func sliceStringArray(a []string, idx int) []string {
	n := make([]string, len(a)-1)
	copy(n, a[:idx])
	copy(n[idx:], a[idx+1:])
	return n
}

func copyStringArray(a []string) []string {
	n := make([]string, len(a))
	copy(n, a)
	return n
}

func sliceInterfaceArray(a []interface{}, idx int) []interface{} {
	n := make([]interface{}, len(a)-1)
	copy(n, a[:idx])
	copy(n[idx:], a[idx+1:])
	return n
}

func copyInterfaceArray(a []interface{}) []interface{} {
	n := make([]interface{}, len(a))
	copy(n, a)
	return n
}

func indexOf(v string, array []string) int {

	idx := -1
	for i, d := range array {
		if d == v {
			idx = i
		}
	}
	if idx < 0 {
		panic(errors.New("not found"))
	}
	return idx

}

type Aggregator func(aggregate, value []interface{}) []interface{}

func key(pt []interface{}) string {
	return fmt.Sprint(pt...)
}
