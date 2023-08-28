package main

import (
	"log"
	"olap-server/cube"
)

func sum(aggregate, value []interface{}) []interface{} {
	s := aggregate[0].(int)
	s += value[0].(int)
	return []interface{}{s}
}

func main() {
	cube := cube.Cubes{
		Dimensions: []string{"location", "time", "convidence", "satelite"},
		Fields:     []string{"total"},
	}

	cube.AddRows([]string{"location", "time", "convidence", "satelite", "total"}, [][]interface{}{
		{"jakarta", 2023, "tinggi", "test", 100},
		{"bandung", 2022, "rendah", "noaa", 15},
		{"jakarta", 2023, "tinggi", "noaa", 10},
		{"bogor", 2022, "sedang", "noaa", 120},
		{"jakarta", 2023, "tinggi", "noaa", 10},
		{"bogor", 2023, "rendah", "noaa", 115},
	})

	// log.Println(cube)
	// log.Println(cube, cube.Dimensions)

	// cube = cube.Slice("time", 2023)
	// log.Println(cube.Headers())

	// cube = cube.Slice("month", "Jan")
	// log.Println(cube)

	cube = cube.Slice("location", 0)
	// cube = cube.RollUp([]string{"time"}, cube.Fields, sum, []interface{}{0})
	// cube = cube.DrillDown("time", 2023, "location")
	log.Println(cube.Headers())

	// api/location?pointer=jakarta&dimension=time

	// fmt.Println(cube.Rows())
	// fmt.Println(cube.Headers())
	/* The following lines will be printed:
	 * apple, 195
	 * orange, 115
	 */
}
