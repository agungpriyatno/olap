package cube

import (
	"errors"
)

// Cube is an OLAP cube
type Cubes struct {
	Dimensions []string        `json:"dimensions,omitempty"`
	Points     [][]interface{} `json:"points,omitempty"`
	Fields     []string        `json:"fields,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
}

func sum(aggregate, value []interface{}) []interface{} {
	s := aggregate[0].(int)
	s += value[0].(int)
	return []interface{}{s}
}

func (c Cubes) DrillDown(dimA string, pointer interface{}, target string) Cubes {
	cube := c.Slice(dimA, pointer)
	return cube.RollUp([]string{target}, c.Fields, sum, []interface{}{0})
}

// Headers return the name of the columns for the Rows method.
func (c Cubes) Headers() []string {

	headers := make([]string, len(c.Dimensions)+len(c.Fields))

	copy(headers, c.Dimensions)
	copy(headers[len(c.Dimensions):], c.Fields)

	return headers
}

// Rows return the Cube content as list of rows. Headers are available via Headers method.
func (c Cubes) Rows() [][]interface{} {

	rows := make([][]interface{}, 0, len(c.Points))

	for i := range c.Points {

		row := make([]interface{}, len(c.Dimensions)+len(c.Fields))

		copy(row, c.Points[i])
		copy(row[len(c.Dimensions):], c.Data[i])

		rows = append(rows, row)
	}

	return rows
}

// IsValid verify that a Cube is properly instentiated
func (c Cubes) IsValid() error {

	if len(c.Data) > 0 {

		for _, pt := range c.Points {
			if len(pt) != len(c.Dimensions) {
				return errors.New("invalid point")
			}
		}

		for _, d := range c.Data {
			if len(d) != len(c.Fields) {
				return errors.New("invalid slice")
			}
		}

		if len(c.Data) != len(c.Points) {
			return errors.New("orphan slices")
		}

	}

	return nil
}

// AddRows add a set of rows to the cube.
func (c *Cubes) AddRows(header []string, rows [][]interface{}) error {

	if len(header) != (len(c.Dimensions) + len(c.Fields)) {
		return errors.New("Invalid header")
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
			return errors.New("Dimension not found")
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
			return errors.New("Dimension not found")
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

// Slice operator picks a rectangular subset of a cube by choosing a single value of its dimensions.
func (c Cubes) Slice(dimension string, value interface{}) Cubes {

	newCube := Cubes{}

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

// Dice operator picks a subcube by choosing a specific values of multiple dimensions.
func (c Cubes) Dice(selector func(c Cubes, idx int) bool) Cubes {

	newCube := Cubes{}
	newCube.Dimensions = copyStringArray(c.Dimensions)
	newCube.Fields = copyStringArray(c.Fields)

	// Points + Data
	for i := range c.Points {
		if selector(c, i) {
			newPt := copyInterfaceArray(c.Points[i])
			newData := copyInterfaceArray(c.Data[i])

			newCube.Points = append(newCube.Points, newPt)
			newCube.Data = append(newCube.Data, newData)
		}
	}

	return newCube
}

// // Aggregator is a summarization method to be used by RollUp operator
// type Aggregator func(aggregate, value []interface{}) []interface{}

// func key(pt []interface{}) string {
// 	return fmt.Sprint(pt...)
// }

// RollUp operator summarize the data along multiple dimensions.
// Ex: rollup(['year','month'], ['flights'], (sum, value) => [sum[0]+value[0]], [0])
func (c Cubes) RollUp(dimensions []string, fields []string, aggregator Aggregator, initialValue []interface{}) Cubes {

	newCube := Cubes{}
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
